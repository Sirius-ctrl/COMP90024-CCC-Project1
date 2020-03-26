from mpi4py import MPI
from utils import lessReader, make_line, process_line
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
        lr = lessReader("smallTwitter.json")
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


def sequential(file_name):
    print("runing on single core sequentially")

    lr = lessReader(file_name)
    header = next(lr)
    line = next(lr)
    lang_acc = Counter()
    
    start = timeit.default_timer()
    while line != "EOF":
        data = json.loads(make_line(line))
        lang = process_line(data)
        lang_acc.update(lang)
        line = next(lr)
    end = timeit.default_timer()

    print("sequential reading takes", end-start)
    print(lang_acc.most_common(10))


def split_reading():

    # setup mpi
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    file_name = "smallTwitter.json"

    if size == 1:
        sequential(file_name)
        return

    # setup file reading object
    lr = lessReader(file_name)
    header = next(lr)

    # setup counter
    lang_acc = Counter()
    hash_tag = Counter()

    file_length = {"tinyTwitter.json": 1000-1,
                   "smallTwitter.json": 5000-1, 
                   "bigTwitter.json": 215443567-1}[file_name]
    
    n_rows = file_length // size
    remaining = 0
    start = n_rows*rank
    end = n_rows*(rank+1)

    if rank == size-1:
        # last core to catch up all
        # note that header has already been removed and we are counting from 0
        totoal_range = range(start, file_length, 1)
    else:
        totoal_range = range(start, end, 1)

    print("i am rank", rank, "which reading from",
          totoal_range[0], "to", totoal_range[-1])

    i = 0
    start = timeit.default_timer()

    for line_num, line in enumerate(lr):
        # reach the end of the file
        if line == "EOF":
            break
        
        if line_num in totoal_range:
            # job start from now
            data = json.loads(make_line(line))
            lang = process_line(data)
            lang_acc.update(lang)
            i += 1
        elif line_num < totoal_range[0]:
            # not yet reach your job
            continue
        else:
            # job done
            break
    
    end = timeit.default_timer()
    print("rank", rank, "has processed", i, "lines", "takes", end-start)
    lang_gather = comm.gather(lang_acc, root=0)
    lang_final = Counter()

    if rank == 0:
        for c in lang_gather:
            lang_final.update(c)
        print(lang_final.most_common(10))


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
    split_reading()
    #main()
    stop = timeit.default_timer()

    if rank == 0:
        print("running takes", stop - start)
