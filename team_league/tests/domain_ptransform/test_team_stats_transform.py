import dataclasses
from typing import List, Dict

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_empty, equal_to
from asgarde.failure import Failure
from dacite import from_dict
from toolz.curried import pipe, map

from team_league.domain.exception.team_stats_validation_exception import TeamStatsValidationException
from team_league.domain.team_stats_raw import TeamStatsRaw
from team_league.domain_ptransform.team_stats_transform import TeamStatsTransform
from team_league.root import ROOT_DIR
from team_league.tests.testing_helper import log_element, load_file_as_dict

TEAM_SLOGANS = {
    "PSG": "Paris est magique",
    "Real": "Hala Madrid"
}


class TestTeamStatsTransform:

    def test_given_input_teams_stats_raw_without_error_when_transform_to_stats_domain_then_expected_output_in_result(
            self):
        with TestPipeline() as p:
            input_teams_stats_raw_file_path = f'{ROOT_DIR}/tests/files/input/input_teams_stats_raw_without_error.json'

            input_teams_stats_raw: List[TeamStatsRaw] = list(
                pipe(load_file_as_dict(input_teams_stats_raw_file_path),
                     map(lambda c: from_dict(data_class=TeamStatsRaw, data=c)))
            )

            input_slogans: PCollection[Dict] = (
                    p
                    | 'Create slogans' >> beam.Create([TEAM_SLOGANS])
            )

            # When.
            result_outputs, result_failures = (
                    p
                    | 'Create team stats raw' >> beam.Create(input_teams_stats_raw)
                    | 'Team stats transform' >> TeamStatsTransform(input_slogans)
            )

            result_outputs_as_dict = (
                    result_outputs
                    | beam.Map(dataclasses.asdict)
            )

            print('################################')
            result_outputs_as_dict | "Print Outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            expected_teams_stats_file_path = f'{ROOT_DIR}/tests/files/expected/expected_teams_stats_without_error.json'
            expected_outputs: List[Dict] = load_file_as_dict(expected_teams_stats_file_path)

            # Then.
            assert_that(result_outputs_as_dict, equal_to(expected_outputs), label='CheckResMatchExpected')
            assert_that(result_failures, is_empty(), label='CheckEmptyFailures')

    def test_given_input_teams_stats_raw_with_one_error_and_one_good_input_when_transform_to_stats_domain_then_one_expected_failure_and_one_good_output(
            self):
        with TestPipeline() as p:
            input_teams_stats_raw_file_path = f'{ROOT_DIR}/tests/files/input/input_teams_stats_raw_with_one_error_one_good_output.json'

            input_teams_stats_raw: List[TeamStatsRaw] = list(
                pipe(load_file_as_dict(input_teams_stats_raw_file_path),
                     map(lambda c: from_dict(data_class=TeamStatsRaw, data=c)))
            )

            input_slogans: PCollection[Dict] = (
                    p
                    | 'Create slogans' >> beam.Create([TEAM_SLOGANS])
            )

            # When.
            result_outputs, result_failures = (
                    p
                    | beam.Create(input_teams_stats_raw)
                    | 'Team stats transform' >> TeamStatsTransform(input_slogans)
            )

            result_outputs_as_dict = (
                    result_outputs
                    | beam.Map(dataclasses.asdict)
            )

            print('################################')
            result_outputs_as_dict | "Print Outputs" >> beam.Map(log_element)
            result_failures | "Print Failures" >> beam.Map(log_element)

            expected_teams_stats_file_path = f'{ROOT_DIR}/tests/files/expected/expected_teams_stats_with_one_error_one_good_output.json'
            expected_outputs: List[Dict] = load_file_as_dict(expected_teams_stats_file_path)

            expected_failures: List[Failure] = [
                Failure(
                    'Validate raw fields',
                    str(input_teams_stats_raw[0]),
                    TeamStatsValidationException(['Team name should not be null or empty'])
                )
            ]

            # Then.
            assert_that(result_outputs_as_dict, equal_to(expected_outputs), label='CheckResMatchExpected')
            assert_that(result_failures, equal_to(
                expected=expected_failures,
                equals_fn=self.check_result_matches_expected), label='CheckFailureMatchExpected')

    def check_result_matches_expected(self, expected_failure: Failure, result_failure: Failure):
        result_input_element: str = result_failure.input_element
        expected_input_element: str = expected_failure.input_element

        result_exception: Exception = result_failure.exception
        expected_exception: Exception = expected_failure.exception

        result_input_equals_expected: bool = result_input_element == expected_input_element
        result_exception_equals_expected: bool = result_exception.errors == expected_exception.errors
        expected_exception_is_validation: bool = type(expected_exception) == TeamStatsValidationException

        return (result_input_equals_expected
                and result_exception_equals_expected
                and expected_exception_is_validation)
