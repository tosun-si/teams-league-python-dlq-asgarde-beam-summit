import json
from typing import Dict

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.io import ReadFromText
from apache_beam.pvalue import PBegin, PCollection

from team_league.application.pipeline_conf import PipelineConf


class TeamSloganJsonFileReadTransform(PTransform):

    def __init__(self,
                 pipeline_conf: PipelineConf):
        super().__init__()
        self.pipeline_conf = pipeline_conf

    def expand(self, input: PBegin) -> PCollection[Dict]:
        return (input |
                "Read Slogan from file" >> ReadFromText(self.pipeline_conf.input_file_slogans) |
                "Map to slogan Dict" >> beam.Map(self.to_slogan_dict))

    def to_slogan_dict(self, slogan_str: str) -> Dict:
        return json.loads(slogan_str)
