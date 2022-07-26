import numpy as np
import pandas as pd
from otlang.sdk.syntax import Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class AddCommand(BaseCommand):
    """
    Make addition of two columns of dataframe
    a, b - columns or numbers must be added
    | add a b - creates a new df

    | add a b as c - creates new column "c" in the old df
    """

    syntax = Syntax(
        [
            Positional("first_argument", required=True, otl_type=OTLType.ALL),
            Positional("second_argument", required=True, otl_type=OTLType.ALL),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress("Start add command")
        # that is how you get arguments
        first_add_argument = self.get_arg("first_argument")
        if isinstance(first_add_argument.value, str):
            first_add = df[first_add_argument.value]
        else:
            first_add = first_add_argument.value

        second_add_argument = self.get_arg("second_argument")
        if isinstance(second_add_argument.value, str):
            second_add = df[second_add_argument.value]
        else:
            second_add = second_add_argument.value
        result_column_name = second_add_argument.named_as

        if isinstance(first_add, (int, float)) and isinstance(second_add, (int, float)):
            if result_column_name != "" and not df.empty:
                first_add = np.array([first_add] * df.shape[0])
                second_add = np.array([second_add] * df.shape[0])
            else:
                first_add = np.array([first_add])
                second_add = np.array([second_add])


        self.logger.debug(f"Command add get first positional argument = {first_add_argument.value}")
        self.logger.debug(
            f"Command add get second positional argument = {second_add_argument.value}"
        )

        if result_column_name != "":
            if not df.empty:
                df[result_column_name] = first_add + second_add
            else:
                df = pd.DataFrame({result_column_name: first_add + second_add})
            self.logger.debug(f"New column name: {result_column_name}")

        else:
            df = pd.DataFrame(
                {
                    f"add_{first_add_argument.value}_{second_add_argument.value}": first_add + second_add
                }
            )
        self.log_progress("Addition is completed.", stage=1, total_stages=1)
        return df
