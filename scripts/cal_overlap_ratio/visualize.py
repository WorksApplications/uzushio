from argparse import ArgumentParser

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def visualize(args):
    if not args.output_path.endswith("/"):
        args.output_path += "/"
    df = pd.read_csv(args.input_path)

    X = []
    Y = []
    overlap_ratio1 = []
    overlap_ratio2 = []
    overlap_ratio3 = []

    for idx, item in df.iterrows():
        X.append(item["dump_1"])
        X.append(item["dump_2"])
        Y.append(item["dump_2"])
        Y.append(item["dump_1"])
        overlap_ratio1.append(item["overlap_ratio1"])
        overlap_ratio2.append(item["overlap_ratio2"])
        overlap_ratio3.append(item["overlap_ratio3"])
        overlap_ratio1.append(item["overlap_ratio2"])
        overlap_ratio2.append(item["overlap_ratio1"])
        overlap_ratio3.append(item["overlap_ratio3"])
        

    for dump in set(df["dump_1"].unique().tolist() + df["dump_2"].unique().tolist()):
        X.append(dump)
        Y.append(dump)
        overlap_ratio1.append(-1)
        overlap_ratio2.append(-1)
        overlap_ratio3.append(-1)
        
    def draw(overlap_ratio, title, output_name, colorscale):

        fig = go.Figure(go.Histogram2d(x=X, 
                                    y=Y, 
                                    z=overlap_ratio, 
                                    colorscale=colorscale,
                                    texttemplate= "%{z:0.2f}", 
                                    histfunc="avg"
                                    )
                        )

        fig.update_layout(
            xaxis=dict(ticks='', showgrid=False, zeroline=False, nticks=30),
            yaxis=dict(ticks='', showgrid=False, zeroline=False, nticks=30),
            xaxis_title="Dump 1",
            yaxis_title="Dump 2",
            title=dict(
                text=title,
                x=0.5,
                y=0.95,
                font=dict(
                    family="Arial",
                    size=15,
                    color='#000000'
                )
            ),
            autosize=False,
            height=1200,
            width=1200,
            hovermode='closest',
        )

        fig.update_xaxes(categoryorder="category ascending", type="category")
        fig.update_yaxes(categoryorder="category ascending", type="category")

        fig.write_image(f"{args.output_path}{output_name}.png")
        fig.show()
        
    draw(overlap_ratio1, "Overlap Ratio of Text Dumps: Intersection Over Dump 1 Size <br> len(dump_1 & dump_2) / len(dump_1)", "overlap_ratio1", "YlGnBu")
    draw(overlap_ratio2, "Overlap Ratio of Text Dumps: Intersection Over Dump 2 Size <br> len(dump_1 & dump_2) / len(dump_2)", "overlap_ratio2", "YlGnBu")
    draw(overlap_ratio3, "Overlap Ratio of Text Dumps: Intersection Over Union of Dump <br> len(dump_1 & dump_2) / len(dump_1 | dump_2) ", "overlap_ratio3", "YlGnBu")
    
def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_path",
        type=str,
        help="Path to the overlap ratio results file.",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="Path for the output figures.",
    )
    args = parser.parse_args()
    visualize(args)
    

if __name__ == "__main__":
    main()
