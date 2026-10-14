import sys

current_category = None
current_sum = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    category, amount = line.split("\t")
    amount = int(amount)

    if current_category == category:
        current_sum += amount
    else:
        if current_category is not None:
            print(f"{current_category}\t{current_sum}")
        current_category = category
        current_sum = amount

if current_category is not None:
    print(f"{current_category}\t{current_sum}")
