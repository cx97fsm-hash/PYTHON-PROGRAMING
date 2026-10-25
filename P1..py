# python programs 

def prog_01():
    """Greeting with input"""
    name = input("Enter your name: ")
    age = int(input("Enter your age: "))
    print(f"Hello {name}! Next year you will be {age + 1}.")


def prog_02():
    """Simple calculator"""
    a = float(input("First number: "))
    b = float(input("Second number: "))
    op = input("Operator (+ - * /): ")

    if op == "+":
        print(a + b)
    elif op == "-":
        print(a - b)
    elif op == "*":
        print(a * b)
    elif op == "/":
        print(a / b if b != 0 else "Cannot divide by zero")
    else:
        print("Invalid operator")


def prog_03():
    """Even or odd"""
    n = int(input("Enter a number: "))
    print("Even" if n % 2 == 0 else "Odd")


def prog_04():
    """Multiplication table"""
    n = int(input("Table of: "))
    for i in range(1, 11):
        print(f"{n} x {i} = {n * i}")


def prog_05():
    """Factorial"""
    n = int(input("Enter a number: "))
    result = 1
    for i in range(2, n + 1):
        result *= i
    print(f"{n}! = {result}")


def prog_06():
    """Fibonacci series"""
    n = int(input("How many terms? "))
    a, b = 0, 1
    for _ in range(n):
        print(a, end=" ")
        a, b = b, a + b
    print()


def prog_07():
    """Prime number checker"""
    n = int(input("Enter a number: "))
    if n < 2:
        print("Not prime")
    else:
        for i in range(2, int(n ** 0.5) + 1):
            if n % i == 0:
                print("Not prime")
                break
        else:
            print("Prime")


def prog_08():
    """Palindrome checker"""
    s = input("Enter text: ").lower().replace(" ", "")
    print("Palindrome" if s == s[::-1] else "Not a palindrome")


def prog_09():
    """Vowel and word counter"""
    text = input("Enter a sentence: ")
    vowels = sum(1 for ch in text.lower() if ch in "aeiou")
    print("Vowels:", vowels)
    print("Words:", len(text.split()))


def prog_10():
    """List operations"""
    nums = [int(x) for x in input("Enter numbers separated by space: ").split()]
    print("Sorted:", sorted(nums))
    print("Max:", max(nums), "| Min:", min(nums))
    print("Sum:", sum(nums), "| Average:", sum(nums) / len(nums))


def prog_11():
    """Student marks (dictionary)"""
    marks = {}
    for _ in range(3):
        subject = input("Subject: ")
        marks[subject] = int(input("Marks: "))

    for sub, m in marks.items():
        print(f"{sub}: {m}")
    print("Percentage:", sum(marks.values()) / len(marks))


def prog_12():
    """Number guessing game"""
    import random

    secret = random.randint(1, 50)
    attempts = 0
    while True:
        guess = int(input("Guess (1-50): "))
        attempts += 1
        if guess < secret:
            print("Too low")
        elif guess > secret:
            print("Too high")
        else:
            print(f"Correct! You took {attempts} attempts.")
            break


def prog_13():
    """Rock, Paper, Scissors"""
    import random

    choices = ["rock", "paper", "scissors"]
    user = input("rock / paper / scissors: ").lower()
    comp = random.choice(choices)
    print("Computer chose:", comp)

    if user == comp:
        print("Draw")
    elif (user, comp) in [("rock", "scissors"), ("paper", "rock"), ("scissors", "paper")]:
        print("You win!")
    else:
        print("You lose!")


def prog_14():
    """Password generator"""
    import random
    import string

    length = int(input("Password length: "))
    chars = string.ascii_letters + string.digits + string.punctuation
    print("Password:", "".join(random.choice(chars) for _ in range(length)))


#  ADVANCED 

def prog_15():
    """To-do list with file handling"""
    FILE = "todo.txt"

    def load():
        try:
            with open(FILE) as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []

    def save(tasks):
        with open(FILE, "w") as f:
            f.write("\n".join(tasks))

    tasks = load()
    while True:
        choice = input("\n1.Add  2.View  3.Remove  4.Exit: ")
        if choice == "1":
            tasks.append(input("Task: "))
        elif choice == "2":
            for i, t in enumerate(tasks, 1):
                print(i, t)
        elif choice == "3":
            i = int(input("Task number: ")) - 1
            if 0 <= i < len(tasks):
                tasks.pop(i)
        elif choice == "4":
            save(tasks)
            break
        save(tasks)


def prog_16():
    """Bank account (OOP)"""

    class BankAccount:
        def __init__(self, owner, balance=0):
            self.owner = owner
            self.balance = balance

        def deposit(self, amount):
            self.balance += amount
            print(f"Deposited {amount}. Balance: {self.balance}")

        def withdraw(self, amount):
            if amount > self.balance:
                print("Insufficient funds")
            else:
                self.balance -= amount
                print(f"Withdrew {amount}. Balance: {self.balance}")

    acc = BankAccount("Suryansh", 1000)
    acc.deposit(500)
    acc.withdraw(2000)
    acc.withdraw(300)


def prog_17():
    """Inheritance"""

    class Animal:
        def __init__(self, name):
            self.name = name

        def speak(self):
            print("Some sound")

    class Dog(Animal):
        def speak(self):
            print(f"{self.name} says Woof!")

    class Cat(Animal):
        def speak(self):
            print(f"{self.name} says Meow!")

    for pet in [Dog("Tommy"), Cat("Kitty")]:
        pet.speak()


def prog_18():
    """Exception handling"""
    while True:
        try:
            a = int(input("Numerator: "))
            b = int(input("Denominator: "))
            print("Result:", a / b)
            break
        except ValueError:
            print("Please enter valid integers.")
        except ZeroDivisionError:
            print("Denominator cannot be zero.")
        finally:
            print("Attempt finished.\n")


def prog_19():
    """Decorator (timing a function)"""
    import time

    def timer(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            print(f"{func.__name__} took {time.time() - start:.4f}s")
            return result
        return wrapper

    @timer
    def slow_sum(n):
        return sum(range(n))

    print(slow_sum(10_000_000))


def prog_20():
    """Generators, comprehensions, lambda"""

    def squares(n):
        for i in range(n):
            yield i ** 2

    print(list(squares(10)))

    nums = range(1, 21)
    evens = [x for x in nums if x % 2 == 0]
    cubes = list(map(lambda x: x ** 3, evens))
    big = list(filter(lambda x: x > 500, cubes))
    print(evens, cubes, big, sep="\n")


# DATA ANALYTICS 

def prog_21():
    """NumPy statistics"""
    import numpy as np

    data = np.array([12, 45, 7, 23, 56, 89, 34])
    print("Mean:", np.mean(data))
    print("Median:", np.median(data))
    print("Std Dev:", np.std(data))
    print("Normalized:", (data - data.mean()) / data.std())


def prog_22():
    """Pandas basics"""
    import pandas as pd

    df = pd.DataFrame({
        "Name": ["Aman", "Riya", "Karan", "Neha", "Simran"],
        "Marks": [78, 92, 65, 88, 71],
        "City": ["Bilaspur", "Shimla", "Bilaspur", "Mandi", "Shimla"],
    })

    print(df.describe())
    print(df[df["Marks"] > 75])
    print(df.groupby("City")["Marks"].mean())


def prog_23():
    """Read a CSV and analyze (needs data.csv in the same folder)"""
    import pandas as pd

    try:
        df = pd.read_csv("data.csv")
    except FileNotFoundError:
        print("data.csv not found. Put a CSV file named data.csv in this folder.")
        return

    print(df.head())
    print(df.info())
    print("Missing values:\n", df.isnull().sum())
    df = df.dropna()
    print(df.describe())


def prog_24():
    """Plot a chart with Matplotlib"""
    import matplotlib.pyplot as plt

    subjects = ["Maths", "Physics", "Python", "English"]
    scores = [82, 74, 91, 68]

    plt.bar(subjects, scores, color="teal")
    plt.title("Semester Scores")
    plt.xlabel("Subject")
    plt.ylabel("Marks")
    plt.show()


def prog_25():
    """Fetch data from an API (JSON)"""
    import requests

    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url, timeout=10)
    users = response.json()

    for u in users[:5]:
        print(u["name"], "-", u["email"], "-", u["address"]["city"])


PROGRAMS = {
    1: prog_01, 2: prog_02, 3: prog_03, 4: prog_04, 5: prog_05,
    6: prog_06, 7: prog_07, 8: prog_08, 9: prog_09, 10: prog_10,
    11: prog_11, 12: prog_12, 13: prog_13, 14: prog_14, 15: prog_15,
    16: prog_16, 17: prog_17, 18: prog_18, 19: prog_19, 20: prog_20,
    21: prog_21, 22: prog_22, 23: prog_23, 24: prog_24, 25: prog_25,
}


def main():
    while True:
        print("\n========== PYTHON PRACTICE MENU ==========")
        for num, func in PROGRAMS.items():
            print(f"{num:>2}. {func.__doc__}")
        print(" 0. Exit")

        try:
            choice = int(input("\nEnter program number: "))
        except ValueError:
            print("Please enter a number.")
            continue

        if choice == 0:
            print("Goodbye!")
            break
        if choice in PROGRAMS:
            print(f"\n--- Running program {choice}: {PROGRAMS[choice].__doc__} ---\n")
            try:
                PROGRAMS[choice]()
            except ModuleNotFoundError as e:
                print(f"Missing library: {e.name}. Install it with: pip install {e.name}")
            except Exception as e:
                print("Error:", e)
        else:
            print("Choose a number between 0 and 25.")


if __name__ == "__main__":
    main()
