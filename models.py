from awscrt import mqtt
from awsiot import iotshadow
import threading
from uuid import uuid4


class LockedData:
    """
    AWS SDK sample implementation of a value with a mutex
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.shadow_value = None
        self.disconnect_called = False
        self.request_tokens = set()


class ReadOnlyNamedShadow:
    """
    Closure class for named shadow event functions, derived from AWS SDK samples
    """
    def __init__(
        self,
        client: iotshadow.IotShadowClient,
        shadow: str,
        thing_name: str,
        default_value: str = "",
    ):
        self.shadow = shadow
        self.thing_name = thing_name
        self.locked_data = LockedData()
        self.client = client
        self.default_value = default_value

    def change_shadow_value(self, value: str):
        """Changes the shadow value both locally and on AWS

        Args:
            value (str): The new value of the device shadow
        """
        with self.locked_data.lock:
            if self.locked_data.shadow_value == value:
                print("Local value is already set.")
                return

            print("Changed local shadow value.")
            self.locked_data.shadow_value = value

    def on_get_shadow_accepted(self, response: iotshadow.GetShadowResponse):
        """Callback for getting the value of the device shadow
        Args:
            response (iotshadow.GetShadowResponse): The response from AWS
        """
        try:
            if response.state:
                if response.state.delta:
                    value = response.state.delta.get(self.shadow)
                    if value:
                        print("  Shadow contains delta value.")
                        self.change_shadow_value(value)
                        return

                if response.state.reported:
                    value = response.state.reported.get(self.shadow)
                    if value:
                        print("  Shadow contains reported value.")
                        self.change_shadow_value(
                            response.state.reported[self.shadow]
                        )
                        return

            print(
                "  Shadow document lacks '{}' property. Setting defaults...".format(
                    self.shadow
                )
            )
            self.change_shadow_value(self.default_value)
            return

        except Exception as e:
            print(e)

    def on_get_shadow_rejected(self, error: iotshadow.GetShadowResponse):
        """Callback for when getting the value of the device shadow doesn't work
        Args:
            error (iotshadow.GetShadowResponse): The response from AWS
        """
        try:
            with self.locked_data.lock:
                try:
                    self.locked_data.request_tokens.remove(error.client_token)
                except KeyError:
                    print(
                        "Ignoring get_shadow_rejected message due to unexpected token."
                    )
                    return

            if error.code == 404:
                print("Thing has no shadow document. Creating with defaults...")
                self.change_shadow_value(self.default_value)
            else:
                exit(
                    "Get request was rejected. code:{} message:'{}'".format(
                        error.code, error.message
                    )
                )

        except Exception as e:
            print(e)

