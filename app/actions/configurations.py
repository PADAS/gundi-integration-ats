from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin, GenericActionConfiguration
import pydantic

from enum import Enum
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, GlobalUISchemaOptions


class FileStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PROCESSED = "processed"


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    data_endpoint: str
    transmissions_endpoint: str

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "data_endpoint",
            "transmissions_endpoint",
        ],
    )


class ProcessObservationsConfig(PullActionConfiguration):
    pass


class FileModel(pydantic.BaseModel):
    filename: str

    @pydantic.validator('filename')
    def validate_filename_extension(cls, value):
        if not value.lower().endswith('.xml'):
            raise ValueError('Filename must have an .xml extension')
        return value


class GetFileStatusConfig(FileModel, GenericActionConfiguration, ExecutableActionMixin):
    pass


class SetFileStatusConfig(FileModel, GenericActionConfiguration, ExecutableActionMixin):
    status: FileStatus = FileStatus.PENDING


class ReprocessFileConfig(FileModel, GenericActionConfiguration, ExecutableActionMixin):
    pass


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
