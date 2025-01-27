To adopt an **iterative Python programming style** that focuses on manual data manipulation first (without error handling or premature functions) and later wraps logic into functions, follow these steps:

---

### **1. Start with Raw, Manual Data Manipulation**
**Goal**: Work directly with data using basic structures (lists, dicts) and loops. Avoid functions until the logic is solid.

#### Example: Sales Data Processing
```python
# Sample raw data (hardcoded for simplicity)
sales_data = [
    {"item": "apple", "price": 0.5, "quantity": 10},
    {"item": "banana", "price": 0.3, "quantity": 15},
    {"item": "orange", "price": 0.4, "quantity": 20},
]

# Step 1: Calculate total sales for each item (manual loop)
for item in sales_data:
    item["total"] = item["price"] * item["quantity"]
    print(f"Total for {item['item']}: ${item['total']}")  # Debug output

# Step 2: Add tax to totals (iterative tweak)
tax_rate = 0.07
for item in sales_data:
    item["total_with_tax"] = item["total"] * (1 + tax_rate)
    print(f"Post-tax total for {item['item']}: ${item['total_with_tax']:.2f}")
```

**Why This Works**:
- No error handling or functions yet.
- Immediate results via `print` for validation.
- Data is manipulated in-place for simplicity.

---

### **2. Refactor into Functions (After Logic Works)**
**Goal**: Once the manual steps are validated, encapsulate logic into reusable functions.

```python
def calculate_total(sales_list):
    for item in sales_list:
        item["total"] = item["price"] * item["quantity"]
    return sales_list

def apply_tax(sales_list, tax_rate):
    for item in sales_list:
        item["total_with_tax"] = item["total"] * (1 + tax_rate)
    return sales_list

# Usage
sales_data = [
    {"item": "apple", "price": 0.5, "quantity": 10},
    # ... (more data)
]

sales_with_total = calculate_total(sales_data)
sales_with_tax = apply_tax(sales_with_total, tax_rate=0.07)
```

**Why This Works**:
- Functions are added **only after** the logic is tested manually.
- Each function has a single responsibility.

---

### **3. Iterate with Minimal Abstraction**
**Avoid Premature Optimization**:
- Don’t create classes or complex helper functions until necessary.
- Use loops and conditionals directly until patterns emerge.

**Example**:
```python
# Before abstraction: Filtering manually
filtered_sales = []
for item in sales_data:
    if item["total"] > 5:
        filtered_sales.append(item)
```

---

### **4. Add Error Handling & Validation Last**
**Goal**: Handle edge cases only after the core logic works.

```python
def calculate_total_safe(sales_list):
    for item in sales_list:
        try:
            item["total"] = item["price"] * item["quantity"]
        except KeyError as e:
            print(f"Missing field: {e}")
    return sales_list
```

---

### **Key Principles**
1. **Manual First**: Use loops, prints, and hardcoded data to validate logic.
2. **Functions Later**: Refactor into functions only when the code works.
3. **Minimalism**: Avoid over-engineering (e.g., classes, decorators) until needed.
4. **Test Iteratively**: Use `print()` or debuggers to check values at each step.

By following this approach, you reduce complexity early and ensure the code works **before** adding structure.