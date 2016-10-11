from datetime import datetime
from math import floor


def read_input(line_max_num=0):
    global inputLine
    read_start = datetime.now()
    print "read userLikeMovies start, " + str(read_start)
    with open("userLikeMovie/part-r-00000", "r") as inputFile:
        # while inputLine != "" and lineNum < 100:
        while inputLine != "":
            inputLine = inputFile.readline()
            line_max_num += 1
            if inputLine != "":
                line_data = inputLine.split(" \x00")
                line_data[1:] = line_data[1].split("\t[")
                line_data[1] = line_data[1].replace("\x00", "")
                line_data[2] = line_data[2].replace("]", "").replace("\n", "").split(", ")

                line_data[0] = int(line_data[0])
                if line_data[1] == "like":
                    line_data[1] = likeKey
                else:
                    line_data[1] = unlikeKey

                for i in range(0, len(line_data[2])):
                    line_data[2][i] = int(line_data[2][i])

                if len(userIds) == 0 or userIds[len(userIds) - 1] != line_data[0]:
                    inputData[line_data[0]] = {}
                    userIds.append(line_data[0])

                inputData[line_data[0]][line_data[1]] = set(line_data[2])
    read_end = datetime.now()
    print "read userLikeMovies end, " + str(read_end) + ", during: " + str(read_end - read_start) + ", from start: " \
          + str(read_end - start)


def get_like(user_id):
    return get_movie(user_id, likeKey)


def get_unlike(user_id):
    return get_movie(user_id, unlikeKey)


def get_movie(user_id, key):
    movie = inputData[user_id]
    if key in movie:
        return movie[key]
    else:
        return set([])


def calculate(len_max=0):
    calculate_start = datetime.now()
    print "calculate start, " + str(calculate_start)
    user_ids_len = len(userIds)
    print "user_ids_len: " + str(user_ids_len)
    if 0 < len_max < user_ids_len:
        calculate_len = len_max
    else:
        calculate_len = user_ids_len

    with open("temp.txt", "w") as outputFile:
        for i in range(0, calculate_len - 1):
            user_i_id = userIds[i]
            l_i = get_like(user_i_id)
            u_i = get_unlike(user_i_id)

            # print ""
            # print "i", i
            # print "userIId", userIId
            # print "Li", Li
            # print "Ui", Ui
            for j in range(i + 1, calculate_len):
                user_j_id = userIds[j]
                l_j = get_like(user_j_id)
                u_j = get_unlike(user_j_id)

                # print "j", j
                # print "userJId", userJId
                # print "Lj", Lj
                # print "Uj", Uj

                # print ""
                # print i, j
                # print "Li&Lj", Li.intersection(Lj)
                # print "Ui&Uj", Ui.intersection(Uj)
                # print "Li||Lj", Li.union(Lj)
                # print "Ui||Uj", Ui.union(Uj)
                # print "(Li&Lj)||(Ui&Uj)", len((Li.intersection(Lj)).union(Ui.intersection(Uj)))
                # print "(Li||Lj)||(Ui||Uj)", len((Li.union(Lj)).union(Ui.union(Uj)))
                numerator = len((l_i & l_j) | (u_i & u_j))
                denominator = len((l_i | l_j) | (u_i | u_j))
                # print str(i) + "-" + str(j), str(numerator) + "/" + str(denominator)
                # print (numerator + 0.0) / denominator
                outputFile.write("\"" + str(i) + "\" \"" + str(j) + "\" " + str((numerator + 0.0) / denominator) + "\n")
            calculate_processing = datetime.now()
            print "calculate processing " + str(i) + " " + str(floor(float(i) / user_ids_len * 1000) / 10) + "%, " + str(calculate_processing) + ", during: "\
                  + str(calculate_processing - calculate_start)\
                  + ", from start: " + str(calculate_processing - start)

    calculate_end = datetime.now()
    print "calculate end, " + str(calculate_end) + ", during: " + str(calculate_end - calculate_start)\
          + ", from start: " + str(calculate_end - start)


start = datetime.now()
print "start, " + str(start)
inputData = {}
userIds = []
inputLine = "a"
likeKey = 1
unlikeKey = -1
if __name__ == "__main__":
    read_input()
    calculate()
