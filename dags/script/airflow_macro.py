import sys


def main(dt):
    print(f"the execution date is : {dt}")
    return dt


if __name__ == "__main__":
    print(f"args values are : {sys.argv[1]}")
    main(sys.argv[1])
