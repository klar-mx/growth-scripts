import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

channels_scores = pd.read_csv('OctoberUserBehaviorAnalysis.csv')
channels_name = channels_scores.Channel.to_list()
print(channels_name)


def generate_radar_graph(labels: list, values: list):
    """
    Function to generate a radar graph
    :param labels: list of labels in the graph
    :param values: list of values to generate de graph
    :return:
    """
    # Array with categories
    categories = [*labels, ""]
    # Channel name
    channel = values[0]
    # Values for the graph
    radar_values = values[1:len(values) - 1]
    radar_values = [*radar_values, radar_values[0]]
    # Labels location
    label_loc = np.linspace(start=0, stop=2 * np.pi, num=len(radar_values))
    # Create figure
    fig = plt.figure(figsize=(8, 8))
    # Add plot
    ax = fig.add_subplot(polar=True)
    # If you want the first axis to be on top:
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    # Tickts positions
    plt.xticks(label_loc, categories)
    for label, i in zip(ax.get_xticklabels(), range(0, len(label_loc))):
        angle_rad = label_loc[i]
        if angle_rad <= np.pi / 2:
            ha = 'left'
            va = "bottom"
            angle_text = angle_rad * (-180 / np.pi) + 90
        elif np.pi / 2 < angle_rad <= np.pi:
            ha = 'left'
            va = "top"
            angle_text = angle_rad * (-180 / np.pi) + 90
        elif np.pi < angle_rad <= (3 * np.pi / 2):
            ha = 'right'
            va = "top"
            angle_text = angle_rad * (-180 / np.pi) - 90
        else:
            ha = 'center'
            va = "bottom"
            angle_text = angle_rad * (-180 / np.pi) - 90
        label.set_rotation(angle_text)
        label.set_verticalalignment(va)
        label.set_horizontalalignment(ha)
    # Draw ylabels
    ax.set_rlabel_position(0)
    # Plot the graph
    ax.plot(label_loc, radar_values, 'o--', color='g', label=channel)
    # Fill the plot
    ax.fill(label_loc, radar_values, alpha=0.15, color='g')
    # Add labels
    ax.set_thetagrids(np.degrees(label_loc), labels=categories)
    # Add title
    plt.title(channel, size=20, y=1.15)
    # Add grid
    plt.grid(True)
    # Tight layout
    plt.tight_layout()
    # Plot legend
    plt.legend()
    # Save figure
    plt.savefig('Radar_Graphs\\' + channel + '.png')
    plt.close()
    return 'Correct graph for: ' + channel


def compare_radar_graph(data: pd.DataFrame, channels: list):
    """
    Create a radar graph with the list of channels given
    :param data: DataFrame to create graphs
    :param channels: List of channels to generate the radar graph
    :return:
    """
    # Create label names
    labels = data.columns[1:5].to_list()
    labels = [x.split(' ')[1] for x in labels]
    # Filter data
    filter_data = data[data.Channel.isin(channels)]
    # Array with categories
    categories = [*labels, ""]
    # Labels location
    label_loc = np.linspace(start=0, stop=2 * np.pi, num=filter_data.shape[1] - 1)
    # Create figure
    fig = plt.figure(figsize=(8, 8))
    # Add plot
    ax = fig.add_subplot(polar=True)
    # If you want the first axis to be on top:
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    # Tickts positions
    plt.xticks(label_loc, categories)
    for label, i in zip(ax.get_xticklabels(), range(0, len(label_loc))):
        angle_rad = label_loc[i]
        if angle_rad <= np.pi / 2:
            ha = 'left'
            va = "bottom"
            angle_text = angle_rad * (-180 / np.pi) + 90
        elif np.pi / 2 < angle_rad <= np.pi:
            ha = 'left'
            va = "top"
            angle_text = angle_rad * (-180 / np.pi) + 90
        elif np.pi < angle_rad <= (3 * np.pi / 2):
            ha = 'right'
            va = "top"
            angle_text = angle_rad * (-180 / np.pi) - 90
        else:
            ha = 'center'
            va = "bottom"
            angle_text = angle_rad * (-180 / np.pi) - 90
        label.set_rotation(angle_text)
        label.set_verticalalignment(va)
        label.set_horizontalalignment(ha)
    # Draw ylabels
    ax.set_rlabel_position(0)
    # Plot the graph
    for index, row in filter_data.iterrows():
        channel = row.to_list()[0]
        radar_values = row.to_list()[1:5]
        radar_values = [*radar_values, radar_values[0]]
        ax.plot(label_loc, radar_values, 'o-', label=channel)
        # Fill the plot
        ax.fill(label_loc, radar_values, alpha=0.15)
    # Add labels
    ax.set_thetagrids(np.degrees(label_loc), labels=categories)
    # Add title
    title = "-".join(channels)
    plt.title(title, size=14, y=1.15)
    # Add grid
    plt.grid(True)
    # Tight layout
    plt.tight_layout()
    # Plot legend
    plt.legend()
    # Save figure
    plt.savefig('Radar_Graphs\\' + "-".join(channels) + '.png')
    plt.close()
    return "Success"


categories = channels_scores.columns[1:5].to_list()
categories = [x.split(' ')[1] for x in categories]

channels_scores.apply(lambda row: generate_radar_graph(categories, row.tolist()), axis=1)
compare_radar_graph(channels_scores, channels=['Organic', 'Referrals', 'Google', 'Facebook'])
compare_radar_graph(channels_scores, channels=['ZoomD', 'Mapeando', 'Apple Search', 'Digital Turbine'])
compare_radar_graph(channels_scores, channels=['Influencers', 'Instagram', 'TikTok', 'Snapchat'])

