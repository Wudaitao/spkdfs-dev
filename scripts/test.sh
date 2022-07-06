#!/bin/bash
#where :
#
#dir_name : the name of the directory to create everything inside. CAUTION if directory already exists it will erase it and create a new empty one!
#num_of_files : the total number of files to be created
#num_of_dirs : the total number of directories to be created
#levels : the number of levels to distribute the directories (the heighest depth of a subdirectory)
#1. Create directories with names with random numbers and characters of length 1-8 and distribute them to the given levels (depth of subdirectories).
#For example if num_of_dirs=5 and levels=3 the directories will be created as:
#thedir/zs2lW3nn
#thedir/zs2lW3nn/pKbkBm
#thedir/zs2lW3nn/pKbkBm/3
#thedir/pUMy8K
#thedir/pUMy8K/Ww4
#
#2. Create files with names with random numbers and characters of length 1-8 and write in them a random string (numbers and characters) with size 1-128 kb. Distribute them in a round-robin order to the subdirectories.
#For example if num_of_files=8 :
#thedir/5Brt7w
#thedir/zs2lW3nn/TtrgE6
#thedir/zs2lW3nn/pKbkBm/791PBb
#thedir/zs2lW3nn/pKbkBm/3/Qg
#thedir/pUMy8K/xIUcAZ9C
#thedir/pUMy8K/Ww4/wvPOWxLF
#thedir/R
#thedir/zs2lW3nn/Ch224y

RED="\033[1;31m"
GREEN="\033[1;32m"
YELLOW="\033[0;33m"
BLUE="\033[1;34m"
MAGENTA="\033[1;35m"
CYAN="\033[0;36m"
RESET="\033[0m" # No Color

echo -e "${BLUE}Directory name:${RESET}   $1"
echo -e "${BLUE}Numbers of files:${RESET} $2"
echo -e "${BLUE}Number of dirs:${RESET}   $3"
echo -e "${BLUE}Levels:${RESET}           $4"
echo

# check if arguments $2, $3 and $4 are non-negative integers
if [ $(($2)) != $2 ] || [ $(($2)) -lt 0 ]
    then
    echo -e ${RED}Invalid number of files${RESET}
    exit
fi
if [ $(($3)) != $3 ] || [ $(($3)) -lt 0 ]
    then
    echo -e ${RED}Invalid number of directories${RESET}
    exit
fi
if [ $(($4)) != $4 ] || [ $(($4)) -lt 0 ]
    then
    echo -e ${RED}Invalid levels${RESET}
    exit
fi

# if dir exists delete it
# actually create file or dir in /tmp
TESTDIR="/tmp$1"
if [ -d "$TESTDIR" ]
    then
    rm -rf $TESTDIR
fi
 create dir
mkdir -p $TESTDIR
./client -mkdir $1

# an array to store paths of created directories
path_array=( $1 )

# create directory hierarchy
echo -e "${GREEN}Directories created:${RESET}"
num_dirs=$(($3))
levels=$(($4))
while [ $num_dirs -gt 0 ]
    do
    dirpath="$1"
    for i in $(seq 1 $levels)
        do
        if [ $num_dirs -gt 0 ]
            then
            # for the length of the dirname
            # find a random number in range 1-8
            length=$((1 + RANDOM % 8))
            # create random alphanumberic strings with length $length
            var="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c $length)"

            # generate random name until there is no directory
            # with same name in same level
            while [ -d "$dirpath/$var" ]
                do
                var="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c $length)"
            done
            dirpath="$dirpath/$var"
            echo $dirpath
            realdirpath="/tmp$dirpath/$var"
            mkdir -p $realdirpath
            #
            ./client -mkdir $dirpath

            path_array+=($dirpath)
            ((num_dirs--))
        fi
    done
done

echo

# create files in round-robin hierarchy
echo -e "${GREEN}Files created:${RESET}"
num_files=$(($2))
while [ $num_files -gt 0 ]
    do
    for i in "${path_array[@]}"
        do
        if [ $num_files -gt 0 ]
            then
            # for the length of the filename
            # find a random number in range 1-8
            length=$((1 + RANDOM % 8))
            # create random alphanumberic strings with length $length
            var="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c $length)"

            # generate random name until there is no file
            # with same name in same level
            while [ -e "$i/$var" ]
                do
                var="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c $length)"
            done
            filepath="/tmp$i/$var"
            uploadpath="$i/$var"
            touch $filepath
            # write in file a random alphanumeric between 128kB-12800kB
            length=$((128000 + RANDOM % 1280000))
            var="$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | head -c $length)"
            echo $var >> $filepath
            echo "filepath $filepath  ($(($length+1)) bytes)"
            #

            echo "uploadpath $uploadpath  "
            ./client -put $filepath $uploadpath

            ((num_files--))
        fi
    done
done

if [ -d "$TESTDIR" ]
    then
    rm -rf $TESTDIR
fi