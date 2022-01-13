import json

from enum import Enum
from subprocess import check_output, CalledProcessError

from charmhelpers.core.hookenv import (
    log,
    status_set,
    DEBUG,
    INFO,
    ERROR,
    WARNING,
)
from charmhelpers.core.host import cmp_pkgrevno


class BasePolicy:
    """Base class to all RabbitMQ policies."""

    # NOTE (gabrielcocenza) Policies with different needs should
    # inherit from this one. The idea is to give flexibility to
    # adapt for every use case. In theory, all policies from
    # RabbitMQ can be set using just this class, but it's strongly
    # advised to use more specific classes because of different
    # versions that can have compatibility issues.

    ApplyToTypes = Enum(
        'ApplyToTypes', 'queues exchanges all', module=__name__
    )

    def __init__(
        self,
        vhost,
        name,
        pattern=None,
        definition=None,
        apply_to="queues",
        priority="1",
        type="generic",
    ):
        """

        :param vhost: name of the virtual host
        :type vhost: str
        :param name: name of the policy
        :type name: str
        :param pattern: regular expression pattern that will be used
            to match queues, exchanges and etc
        :type pattern: str
        :param definition: policy definition (arguments).
            Must be a valid JSON document
        :type definition: str
        :param apply_to: policy should only apply to 'queues',
            'exchanges', or 'all', defaults to 'queues'
        :type apply_to: str, optional
        :param priority: policy priority, defaults to '1'
        :type priority: str, optional
        :param type: type of policy defined in POLICY_HANDLERS,
            defaults to 'generic'
        :type type: str, optional
        """
        self.vhost = str(vhost)
        self.name = str(name)
        self.pattern = str(pattern)
        self.definition = str(definition)
        self.apply_to = str(apply_to)
        self.priority = str(priority)
        self.type = str(type)

    @property
    def apply_to(self):
        return self._apply_to

    @apply_to.setter
    def apply_to(self, value):
        if value in self.ApplyToTypes.__members__:
            self._apply_to = value
        else:
            raise ValueError("Value {} not in ApplyToTypes.".format(value))

    def check(self):
        """Check if policy should ne applied

        :return: By standard returns True, but child classes can override
        this method
        :rtype: Bool
        """
        return True

    def set(self):
        """Set a policy in a specific vhost"""
        if self.check():
            log(
                "Setting policy {} to vhost: '{}' with name: '{}'".format(
                    self.__class__.__name__, self.vhost, self.name
                ),
                level=DEBUG,
            )
            try:
                result = check_output(
                    [
                        "rabbitmqctl",
                        "set_policy",
                        "-p",
                        self.vhost,
                        self.name,
                        self.pattern,
                        self.definition,
                        "--apply-to",
                        self.apply_to,
                        "--priority",
                        self.priority,
                    ]
                ).decode("utf-8")
                log(result, level=INFO)
                return result

            except (CalledProcessError, ValueError) as e:
                msg = "RabbitMQ failed to create policy {}".format(self.name)
                log("{}, {}".format(e, msg), level=ERROR)
                status_set('blocked', msg)
                return None

    def clear(self):
        """Clears a policy by vhost and name"""
        try:
            log(
                "Clearing policy {} to vhost: '{}' with name: '{}'".format(
                    self.__class__.__name__, self.vhost, self.name
                ),
                level=DEBUG,
            )
            result = check_output(
                ["rabbitmqctl", "clear_policy", "-p", self.vhost, self.name]
            ).decode("utf-8")
            log(result, level=INFO)
            return result

        except CalledProcessError as e:
            msg = "RabbitMQ failed to clear policy {}".format(self.name)
            log("{}, {}".format(e, msg), level=ERROR)
            status_set('blocked', msg)
            return None

    def __repr__(self):
        return "{}(vhost: '{}' name: '{}')".format(
            self.__class__.__name__, self.vhost, self.name
        )

    def _key(self):
        return (self.vhost, self.name)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, BasePolicy):
            return self._key() == other._key()
        return NotImplemented


class TTLPolicy(BasePolicy):
    """Time-To-Live and Expiration policy in RabbitMQ.

    More details at https://www.rabbitmq.com/ttl.html
    """

    def __init__(
        self,
        vhost,
        name,
        pattern,
        apply_to="queues",
        priority="1",
        type="ttl",
        ttl=3600000,
        message_ttl=True,
    ):
        """
        :param vhost: name of the virtual host
        :type vhost: str
        :param name: name of the policy
        :type name: str
        :param pattern: regular expression pattern that will be used
            to match queues, exchanges and etc
        :type pattern: str
        :param apply_to: policy should only apply to 'queues',
            'exchanges', or 'all', defaults to 'queues'
        :type apply_to: str, optional
        :param priority: policy priority, defaults to '1'
        :type priority: str, optional
        :param type: type of policy defined in POLICY_HANDLERS,
            defaults to 'ttl'
        :type type: str, optional
        :param ttl: time-to-live in miliseconds, defaults to 3600000
        :type ttl: int, optional
        :param message_ttl: A TTL can be specified on a per-message basis
        using 'expiration' or for a given queue using 'message-ttl',
            defaults to True
        :type message_ttl: bool, optional
        """
        self.vhost = str(vhost)
        self.name = str(name)
        self.pattern = str(pattern)
        self.apply_to = str(apply_to)
        self.priority = str(priority)
        self.type = str(type)
        self.ttl = int(ttl)
        self.message_ttl = bool(message_ttl)

    @property
    def definition(self):
        if self.message_ttl:
            return json.dumps({"message-ttl": self.ttl})

        return json.dumps({"experies": self.ttl})

    def set(self):
        super().set()


class HAPolicy(BasePolicy):
    """Classic queue mirroring policy in RabbitMQ.

    More details at http://www.rabbitmq.com./ha.html
    """

    ModeTypes = Enum('ModeTypes', 'all exactly nodes', module=__name__)
    SyncModeTypes = Enum('SyncModeTypes', 'automatic manual', module=__name__)

    def __init__(
        self,
        vhost,
        name,
        pattern,
        apply_to="queues",
        priority="1",
        params=None,
        type="ha",
        mode="all",
        sync_mode="automatic",
    ):
        """
        :param vhost: name of the virtual host
        :type vhost: str
        :param name: name of the policy
        :type name: str
        :param pattern: regular expression pattern that will be used
            to match queues, exchanges and etc
        :type pattern: str
        :param apply_to: policy should only apply to 'queues',
            'exchanges', or 'all', defaults to 'queues'
        :type apply_to: str, optional
        :param priority: policy priority, defaults to '1'
        :type priority: str, optional
        :param params: values to pass to the policy, possible values
            depend on the mode chosen, defaults to None
        :type params: str, optional
        :param type: type of policy defined in POLICY_HANDLERS,
            defaults to 'ha'
        :type type: str, optional
        :param mode: Valid mode values:
            * 'all': Queue is mirrored across all nodes in the cluster.
                When a new node is added to the cluster, the queue will
                be mirrored to that node.
            * 'exactly': Queue is mirrored to count nodes in the cluster.
            * 'nodes': Queue is mirrored to the nodes listed in node names.
            defaults to 'all'
        :type mode: str, optional
        :param sync_mode: Valid sync_mode values:
            * 'automatic': A queue will automatically synchronise when
                a new mirror joins.
            * 'manual':  A new queue mirror will not receive existing messages.
        :type sync_mode: str, optional
        """

        self.vhost = str(vhost)
        self.name = str(name)
        self.pattern = str(pattern)
        self.apply_to = str(apply_to)
        self.priority = str(priority)
        self.params = params
        self.type = str(type)
        self.mode = str(mode)
        self.sync_mode = str(sync_mode)

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, value):
        if value in self.ModeTypes.__members__:
            self._mode = value
        else:
            raise ValueError("Value {} not in ModeTypes.".format(value))

    @property
    def sync_mode(self):
        return self._sync_mode

    @sync_mode.setter
    def sync_mode(self, value):
        if value in self.SyncModeTypes.__members__:
            self._sync_mode = value
        else:
            raise ValueError("Value {} not in SyncModeTypes.".format(value))

    def check(self):
        """Check if rabbitmq-server version supports mirroring

        :return: False if doesn't support, True if supports.
        :rtype: Bool
        """
        if cmp_pkgrevno("rabbitmq-server", "3.0.0") < 0:
            log(
                (
                    "Mirroring queues cannot be enabled, only supported "
                    "in rabbitmq-server >= 3.0"
                ),
                level=WARNING,
            )
            log(
                (
                    "More information at http://www.rabbitmq.com/blog/"
                    "2012/11/19/breaking-things-with-rabbitmq-3-0"
                ),
                level=INFO,
            )
            return False

        return True

    @property
    def definition(self):
        definition = {"ha-mode": self.mode, "ha-sync-mode": self.sync_mode}
        if self.mode != "all":
            definition['ha-params'] = self.params

        return json.dumps(definition)

    def set(self):
        super().set()


POLICY_HANDLERS = {
    "generic": BasePolicy,
    "ha": HAPolicy,
    "ttl": TTLPolicy,
}
