nums = []
def generate_number_string(number):
    if 1 <= number <= 9:
        return f"{number:03}"
    elif 10 <= number <= 99:
        return f"{number:03}"
    elif 100 <= number <= 113:
        return f"{number:03}"
    else:
        return "범위를 벗어난 숫자입니다."

for num in range(1, 114):
    nums.append(generate_number_string(num))