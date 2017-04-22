#!/bin/bash
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=8

module load Java/1.8.0_71
module load mpj/0.44
javac -cp .:$MPJ_HOME/lib/mpj.jar Twitter_GeoProcssing.java
mpjrun.sh -jar .:$MPJ_HOME/lib/starter.jar -np 8 Twitter_GeoProcssing
mpjrun.sh -np 8 Twitter_GeoProcssing
