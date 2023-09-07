import re
import csv
import time


intro = ""

while intro != "rapid fire" and intro != "search":
    intro = input("Mode: Search or Rapid Fire?")

    if intro.lower().strip() == "rapid fire":
        def rapid():
            main_list = []
            chair_list = []
            minutes_list = []
            apologies_list = []
            print("y - Yes, attended\n"
                  "n - No, didn't attend\n"
                  "a - Apology\n"
                  "c - Chairman\n"
                  "m - Minutes\n"
                  "done/finish/quit/end - to end and print list)")

            with open ("pafra.csv") as file:
                myfile = csv.reader(file)

                for xx in myfile:
                    check = input(f"{xx}?")
                    if check.lower() == "y":
                        main_list.append(xx)
                    elif check.lower() == "c":
                        chair_list.append(xx)
                    elif check.lower() == "m":
                        minutes_list.append(xx)
                    elif check.lower() == "a":
                        apologies_list.append(xx)
                    elif check.lower() == "done" or check.lower() == "finish" or check.lower() == "quit" or check.lower() == "end":
                        break
                    elif check.lower() == "n":
                        pass
                    else:
                        print("Wrong Entry Please Start Again")
                        rapid()

                print(f"\nChair:")
                for chair in chair_list:
                    print(", ".join(chair))

                print(f"\nMinutes:")
                for minutes in minutes_list:
                    print(", ".join(minutes))

                print(f"\nAttendees:")
                for names in main_list:
                    print(", ".join(names))

                print(f"\nApologies:")
                for apologies in apologies_list:
                    print(", ".join(apologies))

                time.sleep(12000)
        rapid()


    # Search Mode

    if intro.lower().strip() == "search":

        print("1. THIS IS CAPS SENSETIVE.\n"
              "2. Enter the first name and the first letter of the surname for each.\n"
              "3. Once you've entered all the Attendees, type done/finish/quit or end to move onto the Apologies.")

        with open("pafra.csv") as file:
            myfile = csv.reader(file)

            ask = ""
            ask2 = ""
            main_list2 = []
            chair_list2 = []
            minutes_list2 = []
            apologies_list2 = []

            chairask = input("Who chaired the meeting?")
            minutesask = input("Who did the minutes?")
            for sections in myfile:
                for names in sections:
                    match = re.search(chairask, names)
                    match2 = re.search(minutesask, names)
                    if match:
                        chair_list2.append(sections)
                    elif match2:
                        minutes_list2.append(sections)

        while ask != "stop" and ask != "done" and ask != "finish" and ask != "quit" and ask != "end":
            with open("pafra.csv") as file:
                myfile = csv.reader(file)
                ask = input("Enter Attendees:")
                for yy in myfile:
                    for zz in yy:
                        matches = re.search(ask, zz)
                        if matches:
                            main_list2.append(yy)

        while ask2 != "stop" and ask2 != "done" and ask2 != "finish" and ask2 != "quit" and ask2 != "end":
            with open("pafra.csv") as file:
                myfile = csv.reader(file)
                ask2 = input("Enter Apologies:")
                for yy2 in myfile:
                    for zz2 in yy2:
                        matches2 = re.search(ask2, zz2)
                        if matches2:
                            apologies_list2.append(yy2)

        print(f"\nChair:")
        for chair2 in chair_list2:
            print(", ".join(chair2))

        print(f"\nMinutes:")
        for minutes2 in minutes_list2:
            print(", ".join(minutes2))

        print(f"\nAttendees:")
        for names in main_list2:
            print(", ".join(names))

        print(f"\nApologies:")
        for apologies in apologies_list2:
            print(", ".join(apologies))

        time.sleep(12000)

# WHAT THIS PROGRAM DOES

# This is a roll call program I made for my wife, it has 2 modes. 1st mode is called rapid fire, it iterates through
# every name in the document and prompts her to say if they attendees, apologies, chaired or took minutes. At the
# end of the list or when the user exist via text command, the program prints a series of appended lists of who attended,
# didn't attend, chaired and took minutes in the format which she requested.
# The 2nd mode is called search, where it prompts the user to type in the names of who did each role, iterates through
# the csv file to collect the name, and prints them in the same fashion as the 1st mode.


# THINGS I COULDN'T FIX YET:

# In the Rapid Fire mode,
# 1. I haven't worked out yet how to make a "user entered a false entry" and prompt the user to enter
# again, instead of starting to entire loop again. That's why I created a class.
# 2. I haven't worked out yet how to skip the first entry when iterating through the CSV file, and how to index when
# iterating through a not callable object in general

# In the Search mode,
# 1. I haven't yet worked out how to return an error saying "the name user entered is not in database", and prompt
# the user to try again