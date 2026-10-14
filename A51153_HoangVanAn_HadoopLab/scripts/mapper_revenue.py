import sys
import csv

# Đọc từng dòng CSV qua DictReader
reader = csv.DictReader(sys.stdin)
for row in reader:
    # Chỉ lấy các đơn hàng thành công SUCCESS
    if row["status"].strip() == "SUCCESS":
        category = row["category"].strip()
        amount = int(row["amount"].strip())
        # Phát sinh cặp Key-Value: category \t amount
        print(f"{category}\t{amount}")
