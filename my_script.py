# Task-0: Print Statement
print("Hello DEB")

# Task-1: Basics Operation
a = 10
b = 20
c = a+b
print(c)

# Task-2: Data structure
# Task-2_01: List
l = [1,2,3,4]
print(l)
print(l[0])

# Task-2_02: Tuple
my_tuple = (1, "hello", 3.14)
print(my_tuple)
print(my_tuple[1])

# Task-2-03: Dict
d = {
    "a": 1,
    "b": 2,
}
print(d)

# Task-2-03_a: Dict Key retrieval
# Alternative 1
print(d["a"])
# Alternative 2: Use .get method
print(d.get("a"))

# Task-2-04: Set
my_set = set()
my_set.add(10)
my_set.add(10)
my_set.add('10')
# Only 10 and '10' will be returned because set only keep a unique value.
# The first 10 is the a value with 'INT' data type while the second is a string as it is wrapped by quote
print(my_set)