from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from datetime import date, timedelta

# Create workbook and active sheet
wb = Workbook()
ws = wb.active

# Define styles
light_green = "C4D79B"
light_orange_fill = PatternFill(start_color=light_green, end_color=light_green, fill_type="solid")  # Light green
light_blue = "DAE9F8"
light_blue_fill = PatternFill(start_color=light_blue, end_color=light_blue, fill_type="solid")  # Light blue
white_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")  # White
bold_font = Font(bold=True)
center_align = Alignment(horizontal="center")
gray = "D9D9D9"
black = "000000"

# Borders
thin_gray_border = Border(left=Side(style='thin', color=gray),
                          right=Side(style='thin', color=gray),
                          top=Side(style='thin', color=gray),
                          bottom=Side(style='thin', color=gray))
thin_black_border = Border(left=Side(style='thin', color=black),
                           right=Side(style='thin', color=black),
                           top=Side(style='thin', color=black),
                           bottom=Side(style='thin', color=black))
black_bottom_border = Border(bottom=Side(style='thin', color=black))  # Black bottom border only

# Headers
ws.append(["Week/Day", "Details"])
ws["A1"].font = bold_font
ws["B1"].font = bold_font
ws["A1"].alignment = center_align
ws["B1"].alignment = center_align

# Apply border and background to header
for cell in ws[1]:
    cell.border = thin_gray_border
    cell.fill = light_orange_fill

# Define the starting date (first day of the year)
start_date = date(2025, 1, 1)  # Adjust this as needed

# Determine the first weekday of the year
first_weekday = start_date.weekday()  # 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
days_in_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# Slice the days_in_week list for the first week
remaining_days = days_in_week[first_weekday:]  # Days starting from the first weekday of the year

# Generate weeks and days
for week in range(1, 53):  # Up to 52 weeks
    # Determine the days for the current week
    current_week_days = remaining_days if week == 1 else days_in_week

    # Calculate the correct start and end dates for the week
    if week == 1:
        week_start = start_date
        week_end = start_date + timedelta(days=len(remaining_days) - 1)
    else:
        week_start = start_date + timedelta(weeks=week - 1, days=-first_weekday)
        week_end = week_start + timedelta(days=4)

    # Format the week label
    week_label = f"Week {week} ({week_start.strftime('%b %-d')} to {week_end.strftime('%b %-d')})"
    ws.append([week_label, ""])
    week_row = ws.max_row

    # Style the week row
    for col in range(1, 3):  # Apply style to both columns (A and B)
        cell = ws.cell(row=week_row, column=col)
        cell.fill = light_orange_fill
        cell.font = bold_font
        cell.alignment = center_align
        # Add black bottom border to both green cells
        cell.border = black_bottom_border if col == 1 or col == 2 else thin_gray_border

    # Add days
    for i, day in enumerate(current_week_days):
        ws.append([day, ""])
        day_row = ws.max_row
        # Alternate row colors: Light blue for even rows, white for odd rows
        fill = light_blue_fill if i % 2 == 0 else white_fill

        # Apply styles for column A (days of the week)
        cell = ws.cell(row=day_row, column=1)
        cell.fill = fill
        cell.border = thin_black_border  # Black border for column A (day cells only)
        cell.font = bold_font
        cell.alignment = Alignment(horizontal="right")

        # Apply styles for column B (details)
        cell = ws.cell(row=day_row, column=2)
        cell.fill = fill
        cell.border = thin_gray_border  # Default gray border for details column

        # Add an empty row after Friday
        if day == "Friday":
            ws.append(["", ""])  # Add blank row
            ws.append(["", ""])  # Add blank row
            empty_row = ws.max_row
            for col in range(1, 3):  # Style the empty row
                cell = ws.cell(row=empty_row, column=col)
                cell.fill = white_fill
                cell.border = thin_gray_border

# Auto-adjust width for Column A
max_length = max(len(str(cell.value)) for cell in ws["A"] if cell.value)
ws.column_dimensions["A"].width = max_length + 2

# Set custom width for the second column (Details)
ws.column_dimensions["B"].width = 150

# Save the file
wb.save("/mnt/c/Users/JHare/Desktop/weekly_tracker.xlsx")
