#!/bin/bash

#SBATCH --partition=physical
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --output=multi_physical_2n8c.out
#SBATCH --mail-user=xinyaon@student.unimelb.edu.au
#SBATCH --mail-type=End
#SBATCH --time=00:15:00
module load Python/3.6.1-intel-2017.u2
time mpirun -n 8 python testCluster.py
