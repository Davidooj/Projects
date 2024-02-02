import re


def arithmatic_arranger(problems, solve = True):
    first = ""
    second = ""
    lines = ""
    calculated = ""

    for equations in problems:
        firstnumber = equations.split(" ")[0]
        operator = equations.split(" ")[1]
        secondnumber = equations.split(" ")[2]

        if len(firstnumber) > 4 or len(secondnumber) > 4:
            print("Error: Numbers cannot be more than four digits")

        elif (re.search(r"[a-zA-Z]", equations)):
            print("Error: Numbers must only contain digits.")

        elif len(problems) > 5:
            print("ERROR: Formatter takes 5 problems maximum")

        elif operator == "/" or operator == "*":
            print("Error: Operator must be '+' or '-'.")

        else:
            length = max(len(firstnumber), len(secondnumber)) + 2
            top_row = firstnumber.rjust(5)
            second_row = operator + secondnumber.rjust(4)
            line = ""
            answer = ""
            for s in range (length):
                line +="-"

            if operator == "+":
                answer = int(firstnumber) + int(secondnumber)
            elif operator == "-":
                answer = int(firstnumber) - int(secondnumber)

            first += top_row + "    "
            second += second_row + "    "
            lines += line + "    "
            calculated += str(answer).rjust(5) + "    "

    if solve == False:
        print(first + "\n" + second + "\n" + lines)
    else:
        print(first + "\n" + second + "\n" + lines + "\n" + calculated)


arithmatic_arranger(["32 + 698", "3801 - 2", "45 + 43", "123 + 49"])