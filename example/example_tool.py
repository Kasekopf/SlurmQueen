import argparse
import sys

"""
A simple tool to exercise some of the features of the SlurmQueen framework.
"""


def write_pair_to_out(key, value):
    print(str(key) + ": " + str(value))


def log(log_string):
    print(log_string, file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("text", type=str, help="Text to repeat")
    parser.add_argument("--a", type=int, help="The first number to add.")
    parser.add_argument("--b", type=int, help="The second number to add")
    args = parser.parse_args()

    log("Information that is printed directly appears in the .out file")
    log("Information written stderr appears in the .log file")
    log("TEST LOGGING: positional argument was " + args.text)

    write_pair_to_out("Sum", args.a + args.b)
    write_pair_to_out("Repeated Text", args.text + args.text + args.text)
