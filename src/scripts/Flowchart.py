import plotly.graph_objects as go

# Define nodes
nodes = [
    "Scraped Vessels",       # 0
    "SEN Flag",              # 1
    "ESP Flag",              # 2
    "no data (flag)",        # 3
    "TSUDA KAIUN",           # 4
    "GRUPO PEREIRA",         # 5
    "GRAND BLEU",            # 6
    "DONGWON INDUSTRIES",    # 7
    "TUNASEN",               # 8
    "No data (ownership)",   # 9
    "ARMADORA PEREIRA"       # 10
]

# Node colors (light grouping by category)
node_colors = [
    "#7f7fff",  # Scraped Vessels
    "#ff9999",  # SEN Flag
    "#66cccc",  # ESP Flag
    "#cccccc",  # no data (flag)
    "#a1dab4",  # TSUDA KAIUN
    "#a1dab4",  # GRUPO PEREIRA
    "#a1dab4",  # GRAND BLEU
    "#a1dab4",  # DONGWON INDUSTRIES
    "#a1dab4",  # TUNASEN
    "#ffcc66",  # No data (ownership)
    "#a1dab4",  # ARMADORA PEREIRA
]

# Define connections (source -> target -> value)
links = [
    # From Scraped Vessels
    (0, 1, 50),  # SEN Flag
    (0, 2, 7),   # ESP Flag
    (0, 3, 184), # no data (flag)

    # From SEN Flag to companies
    (1, 4, 1),
    (1, 5, 1),
    (1, 6, 1),
    (1, 7, 1),
    (1, 8, 1),
    (1, 9, 45),  # No data (ownership)

    # From ESP Flag
    (2, 10, 1),
    (2, 9, 6),   # No data (ownership)
]

# Convert to source, target, value
source = [link[0] for link in links]
target = [link[1] for link in links]
value  = [link[2] for link in links]

# Create Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=30,
        thickness=15,
        line=dict(color="black", width=0.5),
        label=nodes,
        color=node_colors
    ),
    link=dict(
        source=source,
        target=target,
        value=value
    ))])

fig.update_layout(
    title_text="Senegalese and Spanish-Flagged Vessels to Companies",
    font_size=12,
    height=800,
    width=1200
)

fig.show()
