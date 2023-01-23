import argparse
import random
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', nargs='*', type=str, required=True)
    args = parser.parse_args()
    time.sleep(random.random())
    print(args.file)


if __name__ == '__main__':
    main()
