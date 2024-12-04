from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
import pydantic  


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")


class PullObservationsConfig(PullActionConfiguration):
    data_endpoint: str
    transmissions_endpoint: str


class ProcessObservationsConfig(PullActionConfiguration):
    mortality_event_type: str = "mortality_event"
    observations_per_request: int = 200