#! python
"""
This module provides functionality to compare the results of PIM Lucene benchmarks outputs
and generate a diff.

The module contains functions to validate file existence, parse results into a DataFrame,
and compare the results between two DataFrames. The differences are then printed and saved
to a file. The output file is saved in a fwf file in the same directory as the first input
file.

Functions:
    is_valid_file(parser, arg): Checks if the given file exists.
    parse_results(file): Parses the results file and returns a DataFrame.
    compare_results(df1, df2, name1, name2, out_dir): Compares the results between two
        dataframes and prints the differences.
    main(): Parses command line arguments, reads the files, compares the results, and
        saves the differences to a file.

Example:
    Run this module from the command line to compare two files:
    $ python results_diff.py <baseline> <new results>

Note:
    This module requires pandas to be installed.
"""

import argparse
from io import TextIOWrapper
import os
import pandas as pd


def is_valid_file(parser: argparse.ArgumentParser, arg: str) -> TextIOWrapper:
    """
    Check if the given file exists.

    Args:
        parser (argparse.ArgumentParser): The argument parser object.
        arg (str): The file path.

    Returns:
        file: An open file handle.

    Raises:
        ArgumentTypeError: If the file does not exist.
    """
    if not os.path.exists(arg):
        parser.error(f"The file {arg} does not exist!")
    return open(arg, "r", encoding="utf-8")  # return an open file handle


def parse_results(file: os.PathLike) -> pd.DataFrame:
    """
    Parse the results file and return a DataFrame.

    Args:
        file (os.PathLike): The path to the results file.

    Returns:
        pd.DataFrame: The parsed results as a DataFrame.
    """
    df = pd.read_table(
        file,
        header=None,
        names=["lines"],
    )

    df["thread"] = df["lines"].str.extract(r"^THREAD (\d+):", expand=False)
    df["thread"] = df["thread"].ffill()

    df["search"] = df["lines"].str.extract(r'^Searching for: "(.*)"$', expand=False)
    df["search"] = df["search"].ffill()

    df[["doc_id", "score"]] = df["lines"].str.extract(
        r"doc=(\d+) score=([\d.]+)", expand=True
    )
    df["doc_id"] = df["doc_id"].ffill()
    df["score"] = df["score"].ffill()

    df[["rank", "file_name"]] = df["lines"].str.extract(
        r"^(\d+)\. .*wikipedia/files/(.+)", expand=True
    )

    df = df.dropna(subset=["rank"])
    df = df.drop(["lines"], axis=1)

    df["thread"] = df["thread"].astype(int)
    df["doc_id"] = df["doc_id"].astype(int)
    df["score"] = df["score"].astype(float)
    df["rank"] = df["rank"].astype(int)

    return df


def compare_results(
    df1: pd.DataFrame, df2: pd.DataFrame, name1: str, name2: str, out_dir: os.PathLike
) -> None:
    """
    Compare the results between two dataframes and print the differences.

    Args:
        df1 (pd.DataFrame): The first dataframe to compare.
        df2 (pd.DataFrame): The second dataframe to compare.
        name1 (str): Name of the first dataframe.
        name2 (str): Name of the second dataframe.
        out_dir (os.PathLike): Output directory to save the differences.

    Returns:
        None
    """
    merged = pd.merge(
        df1,
        df2,
        how="outer",
        indicator=True,
        on=["thread", "search", "rank"],
        suffixes=(f"_{name1}", f"_{name2}"),
    )
    merged[f"doc_id_{name1}"] = merged[f"doc_id_{name2}"].fillna(-1).astype(int)
    merged[f"doc_id_{name2}"] = merged[f"doc_id_{name2}"].fillna(-1).astype(int)
    differences = merged[
        (merged["_merge"] != "both")
        | (merged[f"doc_id_{name1}"] != merged[f"doc_id_{name2}"])
        | (merged[f"score_{name1}"] != merged[f"score_{name2}"])
        | (merged[f"file_name_{name1}"] != merged[f"file_name_{name2}"])
    ]
    differences = differences.drop(["_merge"], axis=1)
    differences = differences.set_index(["thread", "search", "rank"])

    if differences.empty:
        print("No differences found!")
        return
    print(f"Differences between {name1} and {name2}:")
    print(differences)
    out_path = os.path.join(out_dir, f"{name1}_{name2}_diff.fwf")
    print(f"Saving differences to {out_path}")
    differences.to_string(out_path)


def main() -> None:
    """
    Compare two files and generate a diff of the results.

    This function takes two file paths as input and compares the contents of the files.
    It parses the results from each file, compares them, and generates a diff of the results.
    The diff is saved in the same directory as the first file.

    Args:
        file1 (str): The path to the baseline.
        file2 (str): The path to the new results.
    """

    parser = argparse.ArgumentParser(description="Get both files to compare.")
    parser.add_argument(
        "file1",
        help="First file to compare.",
        metavar="FILE1",
        type=lambda x: is_valid_file(parser, x),
    )
    parser.add_argument(
        "file2",
        help="Second file to compare.",
        metavar="FILE2",
        type=lambda x: is_valid_file(parser, x),
    )

    args = parser.parse_args()

    # name1 = os.path.basename(args.file1.name) + "_1"
    name1 = "baseline"
    # name2 = os.path.basename(args.file2.name) + "_2"
    name2 = "new"

    out_dir = os.path.dirname(args.file1.name)

    df1 = parse_results(args.file1)
    df2 = parse_results(args.file2)

    compare_results(df1, df2, name1, name2, out_dir)


if __name__ == "__main__":
    main()
