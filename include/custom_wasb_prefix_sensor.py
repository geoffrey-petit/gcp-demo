from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from typing import TYPE_CHECKING, Any, Union


from collections.abc import Sequence
from datetime import timedelta

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.sensors.base import BaseSensorOperator


from airflow.providers.microsoft.azure.hooks.wasb import WasbAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CustomWasbPrefixSensorTrigger(BaseTrigger):
    """
    Check for the existence of a blob with the given prefix in the provided container.

    Same as Azure's WasbPrefixSensorTrigger but also logs the detected files with the prefix.
    """

    def __init__(
        self,
        container_name: str,
        prefix: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        public_read: bool = False,
        poke_interval: float = 5.0,
    ):
        if not check_options:
            check_options = {}
        super().__init__()
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_conn_id = wasb_conn_id
        self.check_options = check_options
        self.poke_interval = poke_interval
        self.public_read = public_read

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize WasbPrefixSensorTrigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger",
            {
                "container_name": self.container_name,
                "prefix": self.prefix,
                "wasb_conn_id": self.wasb_conn_id,
                "poke_interval": self.poke_interval,
                "check_options": self.check_options,
                "public_read": self.public_read,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Azure WASB and polls for existence of a blob with given prefix."""
        prefix_exists = False
        hook = WasbAsyncHook(
            wasb_conn_id=self.wasb_conn_id, public_read=self.public_read
        )
        try:
            async with await hook.get_async_conn():
                while not prefix_exists:
                    files_with_prefix = await hook.get_blobs_list_async( # CHANGE
                        container_name=self.container_name,
                        prefix=self.prefix,
                        **self.check_options,
                    )
                    self.log.info("Files with prefix: %s", files_with_prefix) # CHANGE
                    prefix_exists = bool(files_with_prefix)
                    if prefix_exists:
                        message = f"Prefix {self.prefix} found in container {self.container_name}."
                        yield TriggerEvent({"status": "success", "message": message})
                        return
                    else:
                        message = (
                            f"Prefix {self.prefix} not available yet in container {self.container_name}."
                            f" Sleeping for {self.poke_interval} seconds"
                        )
                        self.log.info(message)
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class CustomWasbPrefixSensor(BaseSensorOperator):
    """
    Wait for blobs matching a prefix to arrive on Azure Blob Storage.

    Same as Azure's CustomWasbPrefixSensor but also logs the detected files with the prefix.
    """

    template_fields: Sequence[str] = ("container_name", "prefix")

    def __init__(
        self,
        *,
        container_name: str,
        prefix: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        public_read: bool = False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_options is None:
            check_options = {}
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_conn_id = wasb_conn_id
        self.check_options = check_options
        self.public_read = public_read
        self.deferrable = deferrable

    def poke(self, context) -> bool:
        self.log.info(
            "Poking for prefix: %s in wasb://%s", self.prefix, self.container_name
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        files_with_prefix = hook.get_blobs_list_async( # CHANGE
            self.container_name, self.prefix, **self.check_options
        )
        self.log.info("Files with prefix: %s", files_with_prefix) # CHANGE
        return bool(files_with_prefix)

    def execute(self, context) -> None:
        """
        Poll for state of the job run.

        In deferrable mode, the polling is deferred to the triggerer. Otherwise
        the sensor waits synchronously.
        """
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=CustomWasbPrefixSensorTrigger(
                        container_name=self.container_name,
                        prefix=self.prefix,
                        wasb_conn_id=self.wasb_conn_id,
                        check_options=self.check_options,
                        public_read=self.public_read,
                        poke_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")
