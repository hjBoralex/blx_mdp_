# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 14:13:05 2022

@author: hermann.ngayap
"""
from dash import dcc, html
import plotly.graph_objs as go
from colors import colors
from x_axes import years, quarters, months
from sql_queries import*


BAR_H_WIDTH = 2 
PLOTS_FONT_SIZE = 11
PLOTS_HEIGHT = 340  # For main graphs
SMALL_PLOTS_HEIGHT = 290  # For secondary graphs

year_count = []
for year in query_results_19['année'].unique():
    year_count.append({'label':str(year),'value':year})

annotations = [dict(
            x=xi,
            y=yi,
            text=str(zi),
            xanchor='auto',
            yanchor='bottom',
            showarrow=False,
            align='center', 
            font=dict(size=8),
        ) for xi, yi, zi in zip(quarters['quarters'], query_results_20['prod_merchant_qtr'], query_results_23['coverage_ratio'])]
    
merchant_qtr_bar=html.Div(
    children=[
        #Dropdown Merchant hedged with ppa/quarter
        dcc.Dropdown(id='drop_year_m_ppa_q', options=year_count, value=years['years'].min(),
        style=dict(width='40%',verticalAlign="center", display='inline-block')),
        #Merchant hedged with ppa/quarter
        dcc.Graph(id="m_ppa_cr_q",
                  figure = {'data':[
                      go.Bar(
                           name='HCR', 
                           x=quarters['quarters'], 
                           y=query_results_23['coverage_ratio'],
                           opacity=0,
                           marker=dict(color=colors['white']),
                           marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']),
                           textposition='outside',
                           textfont = dict(family="Times", size= 10, color= colors["white"]),
                              ),
                      go.Bar(
                          name='PPA', 
                          x=quarters['quarters'], 
                          y=query_results_17['ppa_qtr'],
                          opacity=1,
                          marker=dict(color=colors['ppa']),
                          marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color'])
                          ),
                     go.Bar(
                         name='Prod Merchant', 
                         x=quarters['quarters'], 
                         y=query_results_20['prod_merchant_qtr'],
                         opacity=0.25,
                         marker=dict(color=colors['e_white']),                             
                         marker_line=dict(width= BAR_H_WIDTH, color=colors['bar_h_color']) 
                         ), 
                     ], 
                      'layout':go.Layout(title='Prod Merchant hedged with PPA/Quarter',
                                         annotations=annotations,
                                         xaxis=dict(gridcolor=colors['grid'], title='year', dtick=1, tickangle = 45), 
                                         yaxis=dict(gridcolor=colors['grid'], title='GWh', side='left'),
                                         barmode='overlay',
                                         yaxis_range=[0,80],
                                         height=500,
                                         paper_bgcolor = colors["background1"],
                                         plot_bgcolor= colors["background1"],
                                         font=dict(color=colors["text"], size=PLOTS_FONT_SIZE),
                                         showlegend=True,
                                         legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                                         hovermode="x unified")
                      },
                  style={'width': '65%', 'display': 'inline-block', 'vertical-align': 'top'},
                  ),
        ]
    )