import tkinter as tk
import re
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.patches as mpatches
import numpy as np
import matplotlib.colors as mcolors
import sys
def get_topic_partitions(filename):
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
    return  {'brokers':brokers, 'partitions': total_partitions, 'leaders': total_leaders, 'topic_partitions':topic_partitions}

def plot_partition_distribution(topic,panel,topic_partitions):
    # Extract the partition information for the selected topic
    #topic = selected_topic.get()
    fig, ax = plt.subplots(figsize=(5, 5))

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
    wedges, texts = ax.pie(sizes, radius=1-size,colors=outer_colors,wedgeprops=dict(width=0.5))
    #ax.set_title(f'Partition distribution for topic {topic}',loc="")

    # # Add the partition count to the slice labels
    # for i, wedge in enumerate(wedges):
    #     broker = labels[i]
    #     count = partitions[broker]['count']
    #     bbox_props = dict(boxstyle='square', facecolor=outer_colors[i], alpha=0.5)
    #     angle = (wedge.theta2 + wedge.theta1) / 2
    #     x = np.cos(np.deg2rad(angle))
    #     y = np.sin(np.deg2rad(angle))
    #     ax.text(x, y, f'Broker-{broker}\n{count} partitions ({sizes[i]/sum(sizes)*100:.1f}%)', bbox=bbox_props, ha='center', va='center')
    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
          bbox=bbox_props, zorder=0, va="center")
    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        broker = labels[i]
        count = partitions[broker]['count']
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = f"angle,angleA=0,angleB={ang}"
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        ax.annotate(f'Broker-{broker}\n{count} partitions ({sizes[i]/sum(sizes)*100:.1f}%)', xy=(x, y), xytext=(1.35*np.sign(x), 1.4*y),
                horizontalalignment=horizontalalignment, **kw)
    
    
    # Create inner ring
    sizes=np.array([[partitions[broker]['count']-partitions[broker]['count_leader'],partitions[broker]['count_leader']] for broker in labels]).flatten()
    inner_colors=["gray" if i % 2 == 0 else "green" for i in range(len(sizes))]
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

    # textstr = '\n'.join([f"{broker}: {partitions[broker]['count_leader']} leaders" for broker in labels])
    # props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    # ax.text(0.05, 0.95, textstr,  transform=fig.dpi_scale_trans, fontsize=9,
    #     verticalalignment='bottom',  horizontalalignment='left', bbox=props)
    
    is_leader_patch = mpatches.Patch(color='green', label='Leader replicas')
    non_leader_patch = mpatches.Patch(color='gray', label='Non-leader replicas')
    ax.legend(handles=[is_leader_patch, non_leader_patch], loc='upper left',)

# Add the pie chart to the panel
    canvas = plt.gcf().canvas
    canvas.draw()
    plot_widget = FigureCanvasTkAgg(fig, master=panel)
    plot_widget.draw()
    plot_widget.get_tk_widget().pack(side='left', fill='both', expand=True)
    

     # Update the broker label
    # broker_counts = {broker: total_partitions[broker] for broker in brokers}
    # leader_counts = {broker: total_leaders[broker] for broker in brokers}
    # broker_label.config(text=f'Partitions per broker: {broker_counts}   Leaders per broker: {leader_counts}')
def create_broker_detail_text(panel,data, title):
   
    panel.master.update()
     #Update the broker label
    

    labels = list(data.keys())
    values = [data[label] for label in labels]

    broker_counts = {broker: values['total_partitions'][broker] for broker in labels}
    leader_counts = {broker: values['total_leaders'][broker] for broker in labels}
    # Get the size of the panel
    panel_width = panel.winfo_width()
    panel_height = panel.winfo_height()

    # Create a Figure object for the text plot
    fig = plt.Figure(figsize=(panel_width/100, panel_height/100), dpi=100)

    # Create a Text object for the text plot
    # text.pack(side='top', fill='both', expand=1)
    textstr =f'Partitions per broker: {broker_counts}   Leaders per broker: {leader_counts}'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    fontsize = max(panel_width, panel_height) // 20
    fig.text(0.05, 0.95, textstr, fontsize=fontsize,
        verticalalignment='top',  horizontalalignment='center', bbox=props)

    # Create a Canvas widget for the Figure object
    canvas = FigureCanvasTkAgg(fig, master=panel)
    canvas.draw()

    # Place the Canvas widget in the panel
    canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")

    # Set the column and row weight to make the Canvas widget fill the whole panel
    panel.columnconfigure(0, weight=1)
    panel.rowconfigure(0, weight=1)
def create_leader_detail_text(panel, data, title):
    
    
    # Get the size of the panel
    panel_width = panel.winfo_width()
    panel_height = panel.winfo_height()

    # Create a Figure object for the text plots
    fig = plt.Figure(figsize=(20,5), dpi=100)

    # Create a Text object for each broker and add it to the Figure
    y_pos = 1.0
    topic_leader_sum = sum([data[broker]['count_leader'] for broker in data.keys()])
    topic_partitions_sum= sum([data[broker]['count'] for broker in data.keys()])
    sorted_brokers = sorted(data, key=lambda broker: data[broker]['count_leader'],reverse=True)
    for broker in sorted_brokers:
        textstr = f"{data[broker]['count_leader']} leaders\n{round(data[broker]['count_leader']/topic_leader_sum*100,2)} % of topic leaders\n"
        props = dict(boxstyle='round', facecolor='white', alpha=0.5)
        #fontsize = max(panel_width, panel_height) // 30
        fig.text(0.05, y_pos, f"broker-{broker}", verticalalignment='top',  horizontalalignment='left',color='red', fontsize=10, weight='bold')
        fig.text(0.05, y_pos-0.02, textstr, fontsize=10,verticalalignment='top',  horizontalalignment='left')
        y_pos -= 0.08

    fig.text(0.05, y_pos, f"Total partitions: {topic_partitions_sum}\nTotal leaders: {topic_leader_sum}", verticalalignment='top',  horizontalalignment='left',color='red', fontsize=10, weight='bold')
        

    # Adjust the spacing between the subplots to stack them vertically
    fig.subplots_adjust(top=1.0, bottom=0.0)

    # Create a Canvas widget for the Figure object
    canvas = FigureCanvasTkAgg(fig, master=panel)
    # Update the parent frame to ensure proper initialization
    panel.configure(bg='wheat')
    panel.master.update()
    canvas.draw()

    # Place the Canvas widget in the panel
    canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")

    # Set the column and row weight to make the Canvas widget fill the whole panel
    panel.columnconfigure(0, weight=1)
    panel.rowconfigure(0, weight=1)

    
    # Create a text widget and add it to the panel
    # caption = tk.Label(panel)
    # caption.text(tk.END, f"{title}:\n")
    
    
    
    # Prevent the widgets from adapting to the size of the panels
    #text.pack_propagate(False)
# Create a main window with tabs
def plot_pie_chart(panel):
    # Define the data for the pie chart
    labels = ['Broker 1', 'Broker 2', 'Broker 3']
    sizes = [20, 30, 50]

    # Create the pie chart
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')

    # Add a title
    ax.set_title('Distribution of Leaders by Broker')

    # Add the pie chart to the panel
    canvas = plt.gcf().canvas
    canvas.draw()
    plot_widget = FigureCanvasTkAgg(fig, master=panel)
    plot_widget.draw()
    plot_widget.get_tk_widget().pack(side='left', fill='both', expand=True)

def add_tab(topic_partitions,tab_control,selected_topic):
    """Adds a new tab to the tab control."""
    # Create a new tab
    global topic
    topic=selected_topic
    tab = ttk.Frame(tab_control)
    tab_control.add(tab, text=selected_topic)
    tab.grid_columnconfigure(0, weight=1)
    tab.grid_rowconfigure(0, weight=0)
    tab.grid_rowconfigure(1, weight=1)
    
    # Create a panel in the top row to host the "Add Tab" button and combobox
    top_panel = tk.Frame(tab,bg='blue')
    top_panel.grid(row=0, column=0, columnspan=2, sticky='we', padx=5, pady=5)
    top_panel.grid_columnconfigure(0, weight=1)
    top_panel.grid_columnconfigure(1, weight=1)
    
    # Create a Combobox in the top panel
    #selected_topic=list(topic_partitions.keys())[0]
    topic_list = ttk.Combobox(top_panel, textvariable=selected_topic,values=list(topic_partitions.keys()))
    topic_list.grid(row=0, column=0, padx=5, pady=5, sticky='w')
    topic_list.current(list(topic_partitions.keys()).index(selected_topic))

    # Find the length of the longest string in the values option
    max_length = max([len(str(val)) for val in topic_list['values']])

    # Set the width of the Combobox to the length of the longest string plus some padding
    topic_list.configure(width=max_length + 2) 
    
    # Create an "Add Tab" button in the top panel
    add_tab_button = ttk.Button(top_panel, text="Add Tab", command=lambda: add_tab(topic_partitions,tab_control,topic))
    add_tab_button.grid(row=0, column=2, padx=5, pady=5, sticky='e')
    
    message = f"topic : {selected_topic}"
    labelTopic = ttk.Label(top_panel, text=message,background="blue",foreground="white")
    labelTopic.grid(row=0, column=1, padx=5, pady=5, sticky='e')


    # Create two panels in the top row
    top_panel_1 = tk.Frame(tab,borderwidth=1,relief='solid')
    top_panel_2 = tk.Frame(tab,borderwidth=1,relief='solid')
    top_panel_1.grid(row=1, column=0, sticky='nsew', padx=5, pady=5)
    top_panel_2.grid(row=1, column=1, sticky='nsew', padx=5, pady=5)
    tab.grid_columnconfigure(0, weight=4)
    tab.grid_columnconfigure(1, weight=1)
    
    # Create a panel in the bottom row
    #bottom_panel = tk.Frame(tab, bg='green',borderwidth=2,relief='solid')
    #bottom_panel.grid(row=2, column=0, columnspan=2, sticky='nsew', padx=5, pady=5)
    #tab.grid_rowconfigure(2, weight=1)
    
    # Prevent the panels from adapting to the size of the widgets they contain
    top_panel.grid_propagate(False)
    top_panel_1.grid_propagate(False)
    top_panel_2.grid_propagate(False)
    #bottom_panel.grid_propagate(False)
    
    # Set the size of the panels to fill the window
    top_panel_1.config(width=int(screen_width/2*0.8), height=int(screen_height*0.3))
    top_panel_2.config(width=int(screen_width/2*0.2), height=int(screen_height*0.2))
    #bottom_panel.config(width=screen_width, height=int(screen_height*0.1))

    # Set the size of the top panel to fill the width of the window and the height of the button
    top_panel.update_idletasks()
    top_panel.config(width=screen_width, height=add_tab_button.winfo_height()*1.5)
    top_panel.bind('<Configure>', lambda e: top_panel.config(width=screen_width, height=add_tab_button.winfo_height()*1.5))
    tab.update()
    plot_partition_distribution(selected_topic,top_panel_1,topic_partitions)
    #plot text leader details
    # textstr = '\n'.join([f"{broker}: {partitions[broker]['count_leader']} leaders" for broker in labels])
    # props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    # ax.text(0.05, 0.95, textstr,  transform=fig.dpi_scale_trans, fontsize=9,
    #     verticalalignment='bottom',  horizontalalignment='left', bbox=props)
    create_leader_detail_text(top_panel_2,topic_partitions[topic],"Leader details")
    top_panel.update()
    
    tab_control.select(tab)
    
    def clear_panel(panel):
        for child in panel.winfo_children():
            child.destroy()

    def on_resize(event):
        screen_width = event.width
        screen_height = event.height
        # Set the size of the panels to fill the window
        top_panel.config(width=screen_width, height=add_tab_button.winfo_height())
        top_panel_1.config(width=int(screen_width/2*0.8), height=int(screen_height*0.3))
        top_panel_2.config(width=int(screen_width/2*0.2), height=int(screen_height*0.2))
        #bottom_panel.config(width=screen_width, height=int(screen_height*0.1))

    def delayed_resize(event):
    # Call on_resize after 100ms delay
        root.after(100, on_resize, event)
    
    def on_topic_selection(event):
        global topic
        topic=event.widget.get()
        clear_panel(top_panel_1)
        clear_panel(top_panel_2)
        plot_partition_distribution(topic,top_panel_1,topic_partitions)
        create_leader_detail_text(top_panel_2,topic_partitions[topic],"Leader details")
         # Get the current tab object
        current_tab = event.widget.master.master
        # Set the name of the tab to the selected topic
        current_tab_name = topic
        tab_control.tab(current_tab, text=current_tab_name)
         # Refresh the top panel
        labelTopic.configure(text=topic)
        top_panel.update()
        top_panel.config(width=top_panel.winfo_width(), height=add_tab_button.winfo_height()*1.5)
    topic_list.bind("<<ComboboxSelected>>", on_topic_selection)


if len(sys.argv) < 2:
    print("Please provide a file path as an argument.")
    sys.exit()

file_name = sys.argv[1]
root = tk.Tk()
root.title("kafka topics --describe")
# Create a label to display the broker partition and leader count
broker_label = tk.Label(root, text='')
broker_label.pack(side=tk.BOTTOM)

# Get the size of the screen
screen_width = 1000
screen_height = 800
tab_control = ttk.Notebook(root)
add_tab_frame = ttk.Frame(tab_control)
tab_control.add(add_tab_frame, text='', sticky=tk.E)
tab_control.pack(fill='both', expand='yes')

#call get_topic_partitions
parsed_data=get_topic_partitions(file_name)
topics = list(parsed_data['topic_partitions'].keys())
topic_partitions=parsed_data['topic_partitions']
add_tab(tab_control=tab_control,topic_partitions=topic_partitions,selected_topic=topics[0])

 # Update the broker label
brokers = list(topic_partitions.keys())
#broker_counts = {broker: parsed_data['partitions'][broker] for broker in brokers}
#leader_counts = {broker: parsed_data['leaders'][broker] for broker in brokers}
broker_label.config(text=f"Partitions per broker: {parsed_data['partitions']}   Leaders per broker: {parsed_data['leaders']}",fg='white',background='blue')
# Set the font size of the broker_label widget to 12
broker_label.config(font=("Arial", 12))
# Bind the event to update the size of the panels when the window is resized






    #root.bind('<Configure>', delayed_resize)

# Start the main event loop
root.mainloop()
