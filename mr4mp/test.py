from random import choice, randint
from string import ascii_lowercase
from timeit import default_timer as timer

def word():
    return ''.join(choice(ascii_lowercase) for _ in range(7))

def index(id):
    return {w:{id} for w in {word() for _ in range(100)}}

def merge(i, j):
    return {k:(i.get(k,set()) | j.get(k,set())) for k in i.keys() | j.keys()}

if __name__ == '__main__':
    import mr4mp
    pool = mr4mp.pool(2)

    print("Starting.")
    start = timer()
    r = pool.mapreduce(index, merge, range(100))
    print("Finished in " + str(timer()-start) + "s using " + str(len(pool)) + " processes.")
