import logging
import multiprocessing as mp
import asyncio

def double_value(value: int) -> int:
    """
    Doubling the value passed into the function.
    :param value: value given in the function
    """
    return 2 * value

async def return_random_value(delay: int) -> int:
    import random as rand
    await asyncio.sleep(delay)
    return rand.randint(1, 10)


async def main_two():
    task1 = asyncio.create_task(return_random_value(delay=2))
    task2 = asyncio.create_task(return_random_value(delay=2))

    t1 = await task1
    t2 = await task2

    print(t1, t2)


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
    asyncio.run(main_two())





