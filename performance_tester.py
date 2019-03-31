import time
from autodb.index.index import Index

def main():
    index = Index(int)
    start = time.perf_counter()
    for x in range(1000000):
        index.create(x, x)
    print(time.perf_counter() - start)


if __name__ == "__main__":
    main()