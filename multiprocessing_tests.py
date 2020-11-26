import logging
import multiprocessing as mp


def double_value(value: int) -> int:
    """
    Doubling the value passed into the function.
    :param value: value given in the function
    """
    return 2 * value


def main():
    """ Main method to run the script."""

    # creating a pool of processes
    with mp.Pool(processes=5) as p:

        # ordered mapping of the processes
        for i in p.map(double_value, range(10)):
            print(i)

        # unordered mapping of the processes
        for i in p.imap_unordered(double_value, range(10)):
            print(i)


if __name__ == '__main__':
    main()





