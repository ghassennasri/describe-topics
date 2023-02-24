import re
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.patches as mpatches
import numpy as np
import matplotlib.colors as mcolors

# Read the output of `kafka-topics --describe` command from file
with open('describeTopicProdAp', 'r') as f:
    output = f.read()

# Use regular expressions to extract broker and topic information
pattern = re.compile(r'Topic:\s+(\S+)\s+Partition:\s+(\d+)\s+Leader:\s+(\d+)\s+Replicas:\s+(\S+)\s+Isr:\s+(\S+)')
matches = pattern.findall(output)

# Create a dictionary to store the number of partitions on each broker for each topic
topic_partitions = {}
for match in matches:
    topic = match[0]
    replicas = match[3].split(',')
    leader = match[2]
    for replica in replicas:
        broker = replica.split(':')[0]
        if topic not in topic_partitions:
            topic_partitions[topic] = {}
        if broker in topic_partitions[topic]:
            topic_partitions[topic][broker]['count'] += 1
            if( broker == leader):
                topic_partitions[topic][broker]['count_leader']+=1
            
        else:
            if( broker == leader):
                topic_partitions[topic][broker] = {'count': 1, 'count_leader': 1}
            else:
                topic_partitions[topic][broker] = {'count': 1, 'count_leader': 0}



# Compute the total number of partitions and leaders per broker across all topics
brokers = set()
for topic in topic_partitions:
    for broker in topic_partitions[topic]:
        brokers.add(broker)

total_partitions = {broker: 0 for broker in brokers}
total_leaders = {broker: 0 for broker in brokers}
for topic in topic_partitions:
    partitions = topic_partitions[topic]
    for broker in partitions:
        count = partitions[broker]['count']
        count_leader = partitions[broker]['count_leader']
        total_partitions[broker] += count
        total_leaders[broker]+=count_leader
# Create a GUI with a combobox and a button
root = tk.Tk()
root.title("Topic Partition Distribution")
root.geometry("800x600")

# Create a label to display the broker partition and leader count
broker_label = tk.Label(root, text='')
broker_label.pack(side=tk.BOTTOM)

topics = list(topic_partitions.keys())
selected_topic = tk.StringVar(value=topics[0])

topic_label = ttk.Label(root, text="Select a topic:")
topic_label.pack(padx=10, pady=10)


topic_list = ttk.Combobox(root, textvariable=selected_topic, values=topics)
topic_list.pack(padx=10, pady=10)

# Create a figure and a canvas to display the plot
fig = Figure(figsize=(20,5), dpi=100)
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
ax = fig.add_subplot(111)

def plot_partition_distribution(topic):
    # Extract the partition information for the selected topic
    #topic = selected_topic.get()
    ax.clear()

    #canvas.get_tk_widget().delete("all")
    partitions = topic_partitions[topic]
    labels = list(partitions.keys())
    sizes = [partitions[broker]['count'] for broker in labels]
    # Define colors
    cmap = plt.get_cmap("tab20c")
    n_colors = 2000

    # Create a list of colors from the colormap
    outer_colors = [cmap(i) for i in range(n_colors)]

   
    # Plot the partition distribution
    
    size=0.3
    wedges, texts = ax.pie(sizes, radius=1-size,colors=outer_colors)
    ax.set_title(f'Partition distribution for topic {topic}')

    # Add the partition count to the slice labels
    for i, wedge in enumerate(wedges):
        broker = labels[i]
        count = partitions[broker]['count']
        bbox_props = dict(boxstyle='square', facecolor=outer_colors[i], alpha=0.5)
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = np.cos(np.deg2rad(angle))
        y = np.sin(np.deg2rad(angle))
        ax.text(x, y, f'Broker-{broker}\n{count} partitions ({sizes[i]/sum(sizes)*100:.1f}%)', bbox=bbox_props, ha='center', va='center')
    
    
    # Create inner ring
    sizes=np.array([[partitions[broker]['count']-partitions[broker]['count_leader'],partitions[broker]['count_leader']] for broker in labels]).flatten()
    inner_colors=["green" if i % 2 == 0 else "gray" for i in range(len(sizes))]
    inner_wedges, inner_texts = ax.pie(sizes, radius=size, colors=inner_colors,
           wedgeprops=dict(width=size, edgecolor='w'))

    
    inner_texts = [None] * len(sizes)
    for i, wedge in enumerate(inner_wedges):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = np.cos(np.deg2rad(angle))
        y = np.sin(np.deg2rad(angle))
        #broker_name = labels[i]
        if i % 2 == 0:
            inner_color="blue"
        else:
            inner_color="green"
        inner_text = f"{sizes[i]:.0f} part."
        #outer_text += f"\n{outer_percent[i]:.1f}% ({sizes.flatten()[i]:.0f} partitions)"
        #inner_texts[i] = ax.text(x, y, inner_text, ha='center', va='center', color="black")
        #outer_text += f"\n{outer_percent[i]:.1f}% ({sizes.flatten()[i]:.0f} partitions)"
        #inner_texts[i] = ax.text(x, y, inner_text, ha='center', va='center', color="black")
     # Add a legend for the colors
    # Add a text box with the leader count for each broker
    textstr = '\n'.join([f"{broker}: {partitions[broker]['count_leader']} leaders" for broker in labels])
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    ax.text(0.05, 0.95, textstr,  transform=fig.dpi_scale_trans, fontsize=9,
        verticalalignment='bottom',  horizontalalignment='left', bbox=props)
    
    is_leader_patch = mpatches.Patch(color='green', label='Leader replicas')
    non_leader_patch = mpatches.Patch(color='gray', label='Non-leader replicas')
    ax.legend(handles=[is_leader_patch, non_leader_patch], loc='upper left',)


    canvas.draw()
    fig.tight_layout()

     # Update the broker label
    broker_counts = {broker: total_partitions[broker] for broker in brokers}
    leader_counts = {broker: total_leaders[broker] for broker in brokers}
    broker_label.config(text=f'Partitions per broker: {broker_counts}   Leaders per broker: {leader_counts}')

def on_topic_selection(event):
    selected_topic = event.widget.get()
    plot_partition_distribution(selected_topic)

# Plot the pie chart for the selected topic when the window is opened
plot_partition_distribution(selected_topic.get())

# Bind the on_topic_selection function to the topic_list Combobox
topic_list.bind("<<ComboboxSelected>>", on_topic_selection)

root.mainloop()