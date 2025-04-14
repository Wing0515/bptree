import matplotlib.pyplot as plt
import seaborn as sns

# Sample data for each operation (average times in ms)
operations = ['Insert', 'Point Query', 'Range Query', 'Random Query']
data = {
    'Insert': [1747.17, 1761.01, 2527.52, 2473.08, 6722.87, 6508.76],
    'Point Query': [143.418, 144.324, 66.6302, 67.0874, 122.803, 121.944],
    'Range Query': [21.0505, 21.208, 22.3419, 22.3686, 23.5758, 23.5613],
    'Random Query': [170.815, 174.453, 74.6284, 72.5661, 129.887, 128.808]
}

# Set Seaborn color palette for consistent styling
colors = sns.color_palette("muted")

# Function to create a bar chart for a single operation
def create_operation_chart(operation, values):
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bar_width = 0.25  # Width of each bar
    group_positions = [0, 1, 2]  # Positions for 1, 4, and 8 threads
    
    # Plot bars for 1 thread group
    ax.bar(group_positions[0] - bar_width, values[0], width=bar_width, color=colors[0], label="No Prefetching")
    ax.bar(group_positions[0], values[1], width=bar_width, color=colors[1], label="With Prefetching")
    
    # Plot bars for 4 threads group
    ax.bar(group_positions[1] - bar_width, values[2], width=bar_width, color=colors[0])
    ax.bar(group_positions[1], values[3], width=bar_width, color=colors[1])
    
    # Plot bars for 8 threads group
    ax.bar(group_positions[2] - bar_width, values[4], width=bar_width, color=colors[0])
    ax.bar(group_positions[2], values[5], width=bar_width, color=colors[1])
    
    # Add ms values inside the bars with dynamic offset
    for i, val in enumerate(values):
        if i < 2:  # 1 thread
            x_pos = group_positions[0] - bar_width if i == 0 else group_positions[0]
        elif i < 4:  # 4 threads
            x_pos = group_positions[1] - bar_width if i == 2 else group_positions[1]
        else:  # 8 threads
            x_pos = group_positions[2] - bar_width if i == 4 else group_positions[2]
        
        # Dynamic offset: 5% of the bar height below the top
        offset = 0.02 * max(values)
        # Place text inside the bar, centered vertically at the offset position
        ax.text(x_pos, val + offset, f'{val:.2f}', ha='center', va='center', 
                color='black', fontsize=11, fontweight='normal')
    
    # Customize x-axis
    ax.set_xticks(group_positions)
    ax.set_xticklabels(['1 Thread', '4 Threads', '8 Threads'])
    
    # Set labels and title
    ax.set_ylabel('Average Time (ms)')
    ax.set_title(f'{operation} Operation')
    
    # Enlarge y-axis for space
    max_val = max(values)
    ax.set_ylim(0, max_val * 1.5)  # 50% extra space above tallest bar
    
    # Place legend inside chart (upper right)
    ax.legend(loc='upper right')
    
    # Optimize layout
    plt.tight_layout()

# Generate a chart for each operation
for operation in operations:
    create_operation_chart(operation, data[operation])

# Display all charts
plt.show()
