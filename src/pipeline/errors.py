"""Exception hierarchy. Every pipeline-specific failure derives from PipelineError."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Invalid or incomplete configuration."""


class ExtractError(PipelineError):
    """A source could not be read (network, auth, missing file, bad query...)."""


class SchemaError(PipelineError):
    """Incoming data does not match the declared schema (e.g. a required column vanished)."""


class DataQualityError(PipelineError):
    """A dataset-level quality rule with severity=error failed; silver is NOT published."""


class GoldError(PipelineError):
    """A gold model failed to build."""
