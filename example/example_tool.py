import click


@click.command()
@click.option('--a', type=int, help='The first number to add.')
@click.option('--b', type=int, required=False, default=0, help='The second number to add')
@click.option('--output', type=click.File('a', lazy=False), default='-', help='File to write output to')
@click.argument('text', type=str)
def do_stuff(a, b, output, text):
    """
    A simple tool to exercise some of the features of the SlurmQueen framework.
    """
    def output_pair(key, value):
        if output is None:
            print(str(key) + ": " + str(value))
        else:
            output.write(str(key) + ": " + str(value) + "\n")

    print('Information that is printed directly appears in the .log file')
    print('Information written to --output appears in the .out file')
    print('TEST LOGGING: positional argument was ' + str(text))

    output_pair('Sum', a+b)
    output_pair('Repeated Text', text + text + text)


if __name__ == '__main__':
    do_stuff()
