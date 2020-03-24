from mpi4py import MPI
from utils import lessReader, make_line
import json
from mpi4py.futures import MPIPoolExecutor

def main():
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    
    if rank == 0:
        lr = lessReader("smallTwitter.json")
        header = next(lr)
        line = ""
        flag = 0

        while True:
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
        while True:
            comm.send(rank, 0)
            msg = comm.recv(source=0)

            if msg == "EOF":
                comm.send("done", 0)
                print(rank, "finished with", acc)
                break
            else:
                json.loads(make_line(msg))
                acc += 1


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


if __name__ == "__main__":
   main()
