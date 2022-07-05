from enum import Enum


class FailureTableFields(Enum):
    FEATURE_NAME = "featureName"
    JOB_NAME = "jobName"
    PIPELINE_STEP = "pipelineStep"
    INPUT_ELEMENT = "inputElement"
    EXCEPTION_TYPE = "exceptionType"
    STACK_TRACE = "stackTrace"
    COMPONENT_TYPE = "componentType"
    DWH_CREATION_DATE = "dwhCreationDate"
