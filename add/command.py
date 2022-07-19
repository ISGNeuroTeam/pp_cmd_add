import pandas as pd
from otlang.sdk.syntax import Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class AddCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Positional("first_column", required=True, otl_type=OTLType.TEXT),
            Positional("second_column", required=True, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start add command')
        # that is how you get arguments
        first_column = self.get_arg("first_column").value
        second_column_argument = self.get_arg("second_column")
        second_column = second_column_argument.value
        result_column_name = second_column_argument.named_as

        # Make your logic here
        if result_column_name != "":
            addition_df = pd.DataFrame({result_column_name: df[first_column].values + df[second_column].values})
        else:
            addition_df = pd.DataFrame({f"add_{first_column}_{second_column}": df[first_column].values + df[second_column].values})

        # Add description of what going on for log progress
        self.log_progress('Addition is complete.', stage=1, total_stages=2)
        #
        self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        # Use ordinary logger if you need

        self.logger.debug(f'Command add get first positional argument = {first_column}')
        self.logger.debug(f'Command add get second positional argument = {second_column}')

        return addition_df
