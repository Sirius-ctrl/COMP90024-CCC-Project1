from mpi4py import MPI
from utils import lessReader, make_line
import json
from collections import Counter
import timeit

def main():
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if size == 1:
        sequential()
        return
    
    if rank == 0:
        lr = lessReader("tinyTwitter.json")
        header = next(lr)
        line = ""
        flag = 0

        while flag < size-1:
            # all the work are done
            if flag >= size-1:
                print("job done")
                break

            worker = comm.recv()

            if type(worker) is str:
                flag += 1
                continue

            line = next(lr)
            comm.send(line, dest=int(worker))
    else:
        acc = 0
        lang_acc = Counter()
        while True:
            comm.send(rank, 0)
            msg = comm.recv(source=0)

            if msg == "EOF":
                comm.send("done", 0)
                print(rank, "finished with", acc)
                break
            else:
                try:
                    data = json.loads(make_line(msg))
                except:
                    print("error line found, ending with (", msg[:-5], ") before make line")

                lang_acc.update([data['doc']['lang']])
                acc += 1
    
    comm.barrier()

    # now starting to collect the result
    if rank == 0:
        lang_final = Counter()
        for _ in range(size-1):
            lang_final.update(comm.recv())
            
        print(lang_final.most_common(10))
    else:
        comm.send(lang_acc, 0)


def sequential():
    lr = lessReader("tinyTwitter.json")
    header = next(lr)
    line = next(lr)
    lang_acc = Counter()
    
    while line != "EOF":
        data = json.loads(make_line(line))
        lang_acc.update([data['doc']['lang']])
        line = next(lr)
    
    print(lang_acc.most_common(10))

    


############################## following sections are only for testing ##############################

def distributed(data):
    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    data = comm.scatter(data, root=0)
    
    print(data, "rank =", rank)


def shit():
    """
    this work fine
    """
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        i = 0

        while True:
            if i >= size-1:
                break
            s = comm.recv()
            print(s)
            i += 1
    else:
        print("procss", rank, "sent")
        s = comm.send("done"+str(rank), 0)

def shit2():
    """
    this work fine
    """
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        i = 0

        while True:
            if i >= size-1:
                break

            s = comm.recv()
            if "done" in s:
                i += 1
    else:
        acc = 0

        while True:
            if acc == rank:
                comm.send("done"+str(rank), 0)
                break
            else:
                comm.send(str(acc), 0)
                acc += 1
    
    print(rank, "job done")


def shit3():
    """
    this one works
    """
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        i = 0
        memory = 0
        while True:
            if i >= size-1:
                break

            s = comm.recv()

            if type(s) is str:
                i += 1
                continue

            comm.send(memory, int(s))
            memory += 1
    else:
        acc = 0
        while True:
            if acc >= rank:
                comm.send("done"+str(rank), 0)
                break
            else:
                comm.send(rank, 0)
                acc = comm.recv(source=0)
    
    print(rank, "job done")


def shit4():
    """
    this one work, demo for bcast
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        data = {'key1': [7, 2.72, 2+3j],
                'key2': ('abc', 'xyz')}
    else:
        data = None

    data = comm.bcast(data, root=0)
    print("core", rank, "has data", data)


def shit5():
    """
    demo for multiple core share a same reading object
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        lr = lessReader("smallTwitter.json")
        header = next(lr)
    else:
        lr = None

    lr = comm.bcast(lr, root=0)
    print("core", rank, "has lr", len(next(lr)))


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    start = timeit.default_timer()
    main()
    stop = timeit.default_timer()

    if rank == 0:
        print("running takes", stop - start)
