import dash
from flask import Flask
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
#import pandas as pd
from plotly import graph_objs as go
from dash.dependencies import Input, Output, State
import pandas as pd
from plotly.graph_objs import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
#conn = psycopg2.connect(host= "ec2-3-214-216-152.compute-1.amazonaws.com",dbname= "ls", user= "postgres", password="thete456")
#cur = conn.cursor()

def load_data(query):
    conn = psycopg2.connect(host= "ec2-44-237-212-183.us-west-2.compute.amazonaws.com",port = 5431,dbname= "stack_db", user= "postdb", password="db")
    cur = conn.cursor()
    sql_command = (query)
    print (sql_command)

    # Load the data
    data1 = pd.read_sql(sql_command, conn)

    print(data1.shape)
    return (data1)



tags_query="select distinct(tags_distinct) from question_tags_year where tags_distinct in ('sqlite','mongodb','sql-server','python','java',\
            'javascript','asp.net','ios','amazon-web-services','oracle','pandas','amazon-s3','heroku','ngnix','scala','apache-spark','mysql','.net','c','c#',\
            'ruby-on-rails','reactjs');"
#"SELECT DISTINCT(tags_distinct) FROM question_tags_year where tags_distinct in ('python','c#','java');"
tags = load_data(tags_query)

print(tags.head())

tags_dict = [{'label': tag, 'value':tag}  for tag in tags['tags_distinct']]
print(tags_dict[0:5])


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Talent Map'
server = app.server
#fig = px.bar(df, x="month", y="total_count", color="tags_distinct", barmode="group")
mapbox_access_token = 'pk.eyJ1IjoicHJlZXRpLXRhbmVqYSIsImEiOiJja2cyd2gxdTIwNGIxMnhqdDJhZ28wYXF3In0.DZ706pbdvAn5uB_TWzShXg'

#user_loc = "SELECT * ,cast ((total_count/100) as int) as size from user_location_geo where tags_distinct = 'python';"
#df_user = load_data(user_loc)

layout_map = go.Layout(
        height=800,
        autosize=True,
        mapbox_accesstoken='pk.eyJ1IjoicHJlZXRpLXRhbmVqYSIsImEiOiJja2cyd2gxdTIwNGIxMnhqdDJhZ28wYXF3In0.DZ706pbdvAn5uB_TWzShXg',
        mapbox_style="carto-positron",#"open-street-map", #"stamen-watercolor",
        #mapbox_center={"lat": float(location['geoplugin_latitude'][0]), "lon": float(location['geoplugin_longitude'][0])},
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="#7f7f7f"
        )
        #mapbox=dict(
         #   center=dict(
          #      lat=float(location['geoplugin_latitude'][0]),
          #      lon=float(location['geoplugin_longitude'][0])
           # ),
           # pitch=0,
           # zoom=3,
        #)
    )



app.layout = html.Div([

            html.Div([

                html.Div([
                    html.H1(children = 'Talent Map'),

                    html.Div(children = 'Top Tech Talent FootPrints Mapped Here.'
                            )
                ],className ='six columns')
            ], className = "row"),

            html.Div([
                html.Div([

                #html.Div(id='output')

                dcc.Dropdown(
                   id='input-1-state',
                   options= tags_dict,
                   placeholder="Select a Tech Tag",
                   #multi = True
                   )

                ], className="six columns"),

                ],className = "row"),

             html.Div([

                html.Div(
                    [
                        dcc.Graph(id='map-graph')
                                  #animate=True,
                                  #style={'margin-top': '20'})
                    ], className = "six  columns"
                ),


                html.Div([

                html.H4('Tech Progression'),

                dcc.Graph(id='g1')
                ], className="six columns"),

            ], className="row")]

    )



@app.callback([Output('map-graph', 'figure'),
               Output('g1','figure')],
              [Input('input-1-state', 'value')])



def update_figure(input1):
    print(input1)
    query1 = "SELECT sum(total_count) as questions, year from question_tags_year where tags_distinct='" + str(input1) +"' GROUP BY year ORDER BY year;"
    query_output1 = load_data(query1)

    #query2 = "SELECT * ,cast ((total_count/100) as int) as size from user_location_geo where tags_distinct='"+str(input1)+"';"
    query2 = "select cast((CASE WHEN user_reputation BETWEEN 1 AND 100 THEN 1 \
                           WHEN user_reputation BETWEEN 101 AND 1000 THEN 2 \
                           WHEN user_reputation BETWEEN 1001 AND 10000 THEN 3\
                           WHEN user_reputation BETWEEN 10001 AND 30000 THEN 4\
                           WHEN user_reputation BETWEEN 30001 AND 60000 THEN 5\
                           WHEN user_reputation BETWEEN 60001 AND 100000 THEN 6\
                           WHEN user_reputation BETWEEN 100001 AND 300000 THEN 7\
                           WHEN user_reputation BETWEEN 300001 AND 600000 THEN 8\
                           WHEN user_reputation BETWEEN 600001 AND 1000000 THEN 9\
                           ELSE 10 END) as int ) as size, * from user_location_geo where tags_distinct='"+str(input1)+"';"
    query_output2 = load_data(query2)


    return [{"data": [{
                "type": "scattermapbox",
                "lat": list(query_output2['latitude']),
                "lon": list(query_output2['longitude']),
                "hoverinfo": "text",
                "hovertext": [["UserId: {} <br>Location: {} <br>Reputation: {}".format(i,j,k)]
                                for i,j,k in zip(query_output2['user_id'], query_output2['user_location'],query_output2['user_reputation'])],
                "mode": "markers",
                "name": "User",
                "fillcolor":"mediumturquoise",
                "opacity": 0.75,
                "marker":go.scattermapbox.Marker(size =list(query_output2['size']),color ='mediumturquoise',opacity=0.75),
                #"marker": {
                #"size":100,
                # "color" :'mediumturquoise',
                #  "opacity": 0.7}
                }],"layout": layout_map
           },{'data': [{'x': query_output1['year'],'y': query_output1['questions']}], 'layout': { 'xaxis':{ 'title':'Year' }, 'yaxis':{ 'title':'Number of Questions' } }}]


if __name__ == '__main__':
    #app.run_server(debug=True,host="0.0.0.0",port=8080)
    server = app.server
    app.run_server(debug=True, port=8050, host="ec2-52-11-127-56.us-west-2.compute.amazonaws.com")
