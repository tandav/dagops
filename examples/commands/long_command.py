import os
import random
import time


def main():
    if n_iterations := os.environ.get('N_ITERATIONS'):
        n_iterations = int(n_iterations)
    else:
        n_iterations = 10

    for i in range(n_iterations):
        task_name = os.environ['TASK_NAME']
        print(i, time.time(), task_name)
        time.sleep(random.random())


if __name__ == '__main__':
    main()
