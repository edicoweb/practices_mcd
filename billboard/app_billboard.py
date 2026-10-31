import ast
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, ctx, no_update

CARPETA = Path(__file__).resolve().parent
billboard = pd.read_csv(CARPETA / "billboard.csv")
audio = pd.read_csv(CARPETA / "audio_features.csv")


VIOLETA = "#7545F8"
FUCSIA = "#ED378C"
TURQUESA = "#08B8D4"
NARANJA = "#FF963F"
TEXTO = "#22305A"


billboard["fecha"] = pd.to_datetime(billboard["week_id"], errors="coerce")
billboard = billboard[billboard["fecha"].dt.year.between(1958, 2017)].copy()
billboard = billboard.dropna(subset=["song_id", "fecha", "week_position"])
billboard = billboard.sort_values("week_position").drop_duplicates(["song_id", "fecha"])


canciones = billboard.groupby("song_id", as_index=False).agg(
    song=("song", "first"), performer=("performer", "first"),
    debut_year=("fecha", lambda s: s.min().year),
    mejor_puesto=("week_position", "min"),
    semanas_lista=("fecha", "nunique"),
    semanas_top10=("week_position", lambda s: int((s <= 10).sum()))
)
canciones["top10"] = canciones["mejor_puesto"] <= 10


audio["calidad_audio"] = audio[["danceability", "energy"]].notna().sum(axis=1)
audio["tiene_id"] = audio["spotify_track_id"].notna().astype(int)
audio["popularidad_orden"] = audio["spotify_track_popularity"].fillna(-1)
audio = audio.sort_values(
    ["calidad_audio", "tiene_id", "popularidad_orden"],
    ascending=False, kind="mergesort"
).drop_duplicates("song_id")


def genero_general(valor):

    try:
        etiquetas = ast.literal_eval(valor) if isinstance(valor, str) else []
    except (ValueError, SyntaxError):
        etiquetas = []
    if not isinstance(etiquetas, list):
        etiquetas = []
    etiquetas = [str(x).lower() for x in etiquetas]
    if not etiquetas:
        return "Sin etiqueta Spotify"
    if any("hip hop" in x or "rap" in x or "drill" in x for x in etiquetas): return "Hip-hop / rap"
    if any("country" in x for x in etiquetas): return "Country"
    if any("r&b" in x or "soul" in x or "motown" in x for x in etiquetas): return "R&B / soul"
    if any("pop" in x for x in etiquetas): return "Pop"
    if any("rock" in x or "metal" in x or "punk" in x for x in etiquetas): return "Rock"
    if any("latin" in x or "salsa" in x or "reggaeton" in x for x in etiquetas): return "Latino"
    if any("electro" in x or "house" in x or "techno" in x or "edm" in x for x in etiquetas): return "Electrónica"
    if any("jazz" in x or "blues" in x for x in etiquetas): return "Jazz / blues"
    return "Otros géneros"


audio["genero"] = audio["spotify_genre"].apply(genero_general)
canciones = canciones.merge(
    audio[["song_id", "genero", "danceability", "energy"]],
    on="song_id", how="left", validate="one_to_one"
)
canciones["genero"] = canciones["genero"].fillna("Sin datos de Spotify")


semanal = billboard[["song_id", "fecha", "week_position"]].rename(
    columns={"week_position": "puesto"}
).sort_values(["song_id", "fecha"]).reset_index(drop=True)


salto = semanal.groupby("song_id")["fecha"].diff().dt.days.gt(7)
canciones["reingresos"] = canciones["song_id"].map(salto.groupby(semanal["song_id"]).sum()).fillna(0).astype(int)
fila_pico = semanal.loc[semanal.groupby("song_id")["puesto"].idxmin(), ["song_id", "fecha"]]
debut = semanal.groupby("song_id")["fecha"].min()
semanas_pico = (fila_pico.set_index("song_id")["fecha"] - debut).dt.days // 7
canciones["semanas_al_pico"] = canciones["song_id"].map(semanas_pico)


def filtrar(anios, genero):

    d = canciones[canciones["debut_year"].between(anios[0], anios[1])]
    if genero != "Todos":
        d = d[d["genero"] == genero]
    return d


def respuestas_globales():
    c = canciones.assign(decada=canciones["debut_year"] // 10 * 10)
    exitos, resto = c[c["top10"]], c[~c["top10"]]


    r1 = (f"Las canciones que llegaron al Top 10 permanecieron una mediana de "
          f"{exitos['semanas_lista'].median():.0f} semanas en la lista, frente a "
          f"{resto['semanas_lista'].median():.0f} de las demás, y tardaron una mediana de "
          f"{exitos['semanas_al_pico'].median():.0f} semanas desde su debut en alcanzar su mejor puesto. "
          f"El {100 * (c['reingresos'] > 0).mean():.1f} % de las canciones salió del ranking y volvió a entrar.")


    pct = c[c["decada"] >= 1960].groupby("decada")["top10"].mean() * 100
    sem = exitos.groupby("decada")["semanas_top10"].mean()
    dmax = int(pct.idxmax())
    r2 = (f"El porcentaje de debutantes que llegó al Top 10 pasó de {pct[1960]:.1f} % en los años 60 "
          f"a un máximo de {pct[dmax]:.1f} % en los años {str(dmax)[2:]} y bajó a {pct[2010]:.1f} % en los 2010 "
          f"(2017 está incompleto). Una explicación visible en los datos: los éxitos de los 2010 se "
          f"quedaron {sem[2010]:.1f} semanas en el Top 10, frente a {sem[1960]:.1f} en los 60, "
          f"así que quedaron menos lugares para canciones nuevas.")


    g = c.groupby("genero").agg(n=("song_id", "size"), pct=("top10", "mean"))
    g = g[(g["n"] >= 30) & ~g.index.isin(["Sin etiqueta Spotify", "Sin datos de Spotify", "Otros géneros"])]
    g = g.sort_values("pct", ascending=False)
    fmt = lambda nombre: f"{nombre} ({100 * g.loc[nombre, 'pct']:.1f} %, n={g.loc[nombre, 'n']:,})"
    r3 = (f"Mayor proporción de Top 10: {', '.join(fmt(x) for x in g.index[:3])}. "
          f"Menor proporción: {', '.join(fmt(x) for x in g.index[-2:])}. "
          f"Latino y Sin datos de Spotify tienen menos de 30 canciones, así que no se interpretan.")


    def medias(col, datos=c):
        v = datos.dropna(subset=[col])
        return v.loc[v["top10"], col].mean(), v.loc[~v["top10"], col].mean()
    b_si, b_no = medias("danceability")
    e_si, e_no = medias("energy")
    b2_si, b2_no = medias("danceability", c[c["debut_year"] >= 2000])
    r4 = (f"La bailabilidad media es {b_si:.3f} en las canciones Top 10 frente a {b_no:.3f} en las demás; "
          f"desde 2000 la diferencia crece ({b2_si:.3f} frente a {b2_no:.3f}). La energía casi no las distingue "
          f"({e_si:.3f} frente a {e_no:.3f}). En la dispersión ambos grupos se superponen: no hay una "
          f"fórmula musical del éxito, y estas diferencias no prueban causalidad.")
    return [("01 · Trayectoria", r1), ("02 · Tendencia", r2), ("03 · Géneros", r3), ("04 · Audio", r4)]


RESPUESTAS = respuestas_globales()


GENEROS = sorted(canciones["genero"].unique())


def canciones_destacadas(datos, incluir=None):

    mejores = datos.sort_values(
        ["semanas_top10", "mejor_puesto", "semanas_lista"],
        ascending=[False, True, False]
    ).head(100)
    if incluir is not None and incluir not in set(mejores["song_id"]):
        mejores = pd.concat([datos[datos["song_id"] == incluir], mejores])
    return [{"label": f"{r.song} — {r.performer} ({r.debut_year})", "value": r.song_id}
            for r in mejores.itertuples(index=False)]


def estilo_grafico(fig, altura=390, margen=None):

    fig.update_layout(
        height=altura, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#F7F8FF",
        font=dict(family="Arial, sans-serif", color=TEXTO, size=12),
        title=dict(font=dict(size=17, color="#22256B"), x=0.025, xanchor="left"),
        margin=margen or dict(l=55, r=22, t=66, b=55),
        hoverlabel=dict(bgcolor="#262568", font_color="white"),
        legend=dict(orientation="h", y=1.14, x=0, title_text="")
    )
    fig.update_xaxes(showgrid=True, gridcolor="#E4E8F7", zeroline=False, linecolor="#CCD3EA")
    fig.update_yaxes(showgrid=True, gridcolor="#E4E8F7", zeroline=False, linecolor="#CCD3EA")
    return fig


def figura_vacia(titulo):
    fig = go.Figure()
    fig.add_annotation(text="No hay datos para esta selección", showarrow=False,
                       font=dict(size=15, color="#6A7194"))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return estilo_grafico(fig.update_layout(title=titulo))


app = Dash(__name__, assets_folder=str(CARPETA / "assets"))
app.title = "Billboard | Explorador musical"


inicial = canciones_destacadas(canciones)


def tarjeta(id_, titulo, icono, clase):
    return html.Div(className=f"kpi {clase}", children=[
        html.Div([html.Span(icono, className="kpi-icon"), html.Span(titulo, className="kpi-title")]),
        html.Div(id=id_, className="kpi-number")
    ])


def panel(pregunta, pista, grafico):

    return html.Section(className="chart-card", children=[
        html.P(pregunta, className="chart-question"),
        html.P(pista, className="chart-hint"),
        dcc.Graph(
            id=grafico,
            responsive=True,
            config={"displaylogo": False, "responsive": True}
        )
    ])


app.layout = html.Div(className="page", children=[
    html.Div(className="shell", children=[
        html.Header(className="hero", children=[
            html.Div("♪  VISUALIZACIÓN DE DATOS · PROYECTO FINAL", className="eyebrow"),
            html.H1(["Billboard ", html.Span("Music Explorer", className="hero-highlight")]),
            html.P("¿Cómo ha evolucionado el éxito de las canciones en Billboard Hot 100 y qué "
                   "características musicales comparten aquellas que alcanzan las mejores posiciones?",
                   className="hero-question"),
            html.Div([html.Span("1958–2017"), html.Span("4 vistas enlazadas"),
                      html.Span("2 CSV originales")], className="hero-tags")
        ]),
        html.Section(className="filters", children=[
            html.Div(className="filters-title", children=["✦  Explora los datos", html.Small("Modifica los filtros para actualizar las cuatro vistas")]),
            html.Div(className="filter-grid", children=[
                html.Div([html.Label("Año de debut"),
                          dcc.RangeSlider(1958, 2017, 1, value=[1958, 2017],
                                          marks={i: str(i) for i in [1958, 1970, 1980, 1990, 2000, 2010, 2017]},
                                          id="f_anios", allowCross=False, tooltip={"placement": "bottom"})], className="filter-year"),
                html.Div([html.Label("Género musical"),
                          dcc.Dropdown(id="f_genero", options=[{"label": "Todos los géneros", "value": "Todos"}] +
                                       [{"label": g, "value": g} for g in GENEROS],
                                       value="Todos", clearable=False)]),
                html.Div([html.Label("Canción para seguir su trayectoria"),
                          dcc.Dropdown(id="f_cancion", options=inicial,
                                       value=inicial[0]["value"], clearable=False)])
            ])
        ]),
        html.Div(className="kpi-grid", children=[
            tarjeta("k_canciones", "CANCIONES ANALIZADAS", "♫", "kpi-purple"),
            tarjeta("k_top10", "ALCANZARON TOP 10", "★", "kpi-pink"),
            tarjeta("k_audio", "CON DATOS DE AUDIO", "◉", "kpi-cyan")
        ]),
        html.P(id="estado", className="status"),
        html.Div(className="charts", children=[
            panel("01 · ¿Cómo cambia el puesto de una canción semana a semana?",
                  "Cambia la canción con el menú o con un clic en un punto del gráfico 4.", "g_trayectoria"),
            panel("02 · ¿Cómo ha variado el éxito de las canciones debutantes?",
                  "Pasa el cursor para ver cuántas canciones hay detrás de cada porcentaje.", "g_anual"),
            panel("03 · ¿Qué géneros reúnen mayor proporción de éxitos? (* muestra menor de 30)",
                  "Haz clic en una barra para filtrar ese género en todo el dashboard.", "g_generos"),
            panel("04 · ¿Qué relación existe entre energía, bailabilidad y Top 10?",
                  "Dibuja un rectángulo para seleccionar canciones; doble clic para quitarlo.", "g_audio")
        ]),
        html.Section(className="insight", children=[
            html.Div("✦ HALLAZGO DEL SEGMENTO", className="insight-label"),
            html.P(id="hallazgo", className="insight-text")
        ]),
        html.Section(className="answers", children=[
            html.Div(["✦ RESPUESTAS DEL ANÁLISIS", html.Small("datos completos 1958–2017, sin filtros")],
                     className="answers-label"),
            html.Div(className="answers-grid", children=[
                html.Div(className="answer", children=[html.H3(t), html.P(texto)]) for t, texto in RESPUESTAS
            ])
        ]),
        html.Footer(className="foot", children=[
            html.P("Interacción: el rectángulo en el gráfico 4 recalcula los KPI, los gráficos 2 y 3 y el hallazgo. "
                   "Un clic en un punto del gráfico 4 muestra su trayectoria en el gráfico 1 (estrella naranja). "
                   "Un clic en una barra del gráfico 3 aplica ese género como filtro."),
            html.P("Alcanzar Top 10 = llegar alguna vez a los puestos 1–10 dentro de 1958–2017. "
                   "1958 comienza en agosto; ¹ las canciones de 2017 no se siguen después de diciembre. "
                   "El género se estima mediante etiquetas de Spotify; no todos los registros tienen audio. "
                   "Los patrones observados no demuestran causalidad.")
        ])
    ])
])


@app.callback(Output("f_cancion", "options"), Output("f_cancion", "value"),
              Output("g_audio", "clickData"),
              Input("f_anios", "value"), Input("f_genero", "value"),
              Input("g_audio", "clickData"), State("f_cancion", "value"))
def cambiar_lista(anios, genero, clic, elegida):
    d = filtrar(anios, genero)
    disponibles = set(d["song_id"])
    por_clic = ctx.triggered_id == "g_audio"
    if por_clic and clic and clic.get("points"):
        dato = clic["points"][0].get("customdata")
        id_clic = dato[0] if isinstance(dato, list) and dato else dato
        if id_clic in disponibles:
            elegida = id_clic
    if elegida not in disponibles:
        elegida = None
    opciones = canciones_destacadas(d, elegida)
    if elegida is None and opciones:
        elegida = opciones[0]["value"]

    return opciones, elegida, (None if por_clic else no_update)


@app.callback(Output("f_genero", "value"), Output("g_generos", "clickData"),
              Input("g_generos", "clickData"), prevent_initial_call=True)
def clic_genero(clic):
    if not clic or not clic.get("points"):
        return no_update, no_update
    genero = clic["points"][0].get("y")

    return (genero if genero in GENEROS else no_update), None


@app.callback(
    Output("k_canciones", "children"), Output("k_top10", "children"),
    Output("k_audio", "children"), Output("estado", "children"),
    Output("g_trayectoria", "figure"), Output("g_anual", "figure"),
    Output("g_generos", "figure"), Output("g_audio", "figure"),
    Output("hallazgo", "children"), Output("g_audio", "selectedData"),
    Input("f_anios", "value"), Input("f_genero", "value"),
    Input("f_cancion", "value"), Input("g_audio", "selectedData")
)
def actualizar(anios, genero, id_cancion, seleccion):
    base = filtrar(anios, genero).copy()


    por_seleccion = ctx.triggered_id == "g_audio"
    ids = {p["customdata"][0] for p in (seleccion or {}).get("points", []) if p.get("customdata")}
    seleccion_valida = por_seleccion and bool(ids)
    d = base[base["song_id"].isin(ids)].copy() if seleccion_valida else base
    if seleccion_valida and d.empty:
        seleccion_valida, d = False, base
    nueva_seleccion = no_update if por_seleccion else None

    n = len(d)
    exito = 100 * d["top10"].sum() / n if n else 0
    con_audio = int(d[["energy", "danceability"]].notna().all(axis=1).sum())
    k1 = f"{n:,}"
    k2 = f"{exito:.1f} %"
    k3 = f"{con_audio:,}"
    etiqueta_genero = "Todos los géneros" if genero == "Todos" else genero
    estado = (f"● Selección activa: {n:,} canciones. Doble clic en el gráfico 4 para quitarla."
              if seleccion_valida else
              f"● Período de debut {anios[0]}–{anios[1]} · {etiqueta_genero} · {n:,} canciones."
              + (" La selección no contenía puntos; se conserva el filtro general."
                 if por_seleccion and seleccion is not None and not ids else ""))


    elegida = base[base["song_id"] == id_cancion]
    if elegida.empty:
        f1 = figura_vacia("1. Trayectoria semanal")
    else:
        r = elegida.iloc[0]
        s = semanal[semanal["song_id"] == id_cancion].sort_values("fecha").copy()
        brechas = s["fecha"].diff().dt.days.gt(7)
        cortes = pd.DataFrame({"fecha": s.loc[brechas, "fecha"] - pd.Timedelta(days=7), "puesto": float("nan")})
        s = pd.concat([s[["fecha", "puesto"]], cortes], ignore_index=True).sort_values("fecha")
        nombre = f"{r['song']} — {r['performer']}"
        titulo = nombre if len(nombre) <= 51 else nombre[:48] + "..."
        f1 = px.line(s, x="fecha", y="puesto", markers=True,
                     title=f"1. {titulo}",
                     labels={"fecha": "Semana", "puesto": "Posición en Billboard"})
        f1.update_traces(line=dict(color=VIOLETA, width=3), marker=dict(size=6, color=FUCSIA), connectgaps=False)
        f1.update_yaxes(range=[100, 1], tickvals=[1, 10, 20, 40, 60, 80, 100])
        f1.add_hline(y=10, line_dash="dot", line_color=FUCSIA,
                     annotation_text="Top 10", annotation_font_color=FUCSIA)
        pico = s.loc[s["puesto"].idxmin()]

        f1.add_annotation(x=pico["fecha"], y=pico["puesto"], ax=60, ay=120, arrowhead=2,
                          arrowcolor=NARANJA, font=dict(color=TEXTO, size=12), bgcolor="#FFF4EA",
                          text=f"Mejor puesto: #{int(pico['puesto'])} · {int(r['semanas_lista'])} semanas en lista")
        f1 = estilo_grafico(f1)


    por_anio = d.groupby("debut_year", as_index=False).agg(total=("song_id", "size"), top10=("top10", "sum"))
    if por_anio.empty:
        f2 = figura_vacia("2. Top 10 por año de debut")
    else:
        por_anio["porcentaje"] = por_anio["top10"] / por_anio["total"] * 100
        permanencia = d[d["top10"]].groupby("debut_year", as_index=False)["semanas_top10"].mean()
        por_anio = por_anio.merge(permanencia, on="debut_year", how="left")
        f2 = px.line(por_anio, x="debut_year", y="porcentaje", markers=True,
                     hover_data={"total": True, "top10": True, "porcentaje": ":.1f", "semanas_top10": ":.1f"},
                     title="2. % que llegó al Top 10 por año",
                     labels={"debut_year": "Año de debut", "porcentaje": "% que llegó al Top 10",
                             "total": "Canciones debutantes", "top10": "Llegaron al Top 10",
                             "semanas_top10": "Semanas promedio en Top 10 (éxitos)"})
        f2.update_traces(line=dict(color=TURQUESA, width=3), marker=dict(color=VIOLETA, size=5))
        if 2017 in por_anio["debut_year"].values:
            f2.add_vrect(x0=2016.5, x1=2017.5, fillcolor=NARANJA, opacity=0.10, line_width=0,
                         annotation_text="2017¹", annotation_position="top left")
        f2.update_yaxes(rangemode="tozero")
        f2 = estilo_grafico(f2)


    por_genero = d.groupby("genero", as_index=False).agg(total=("song_id", "size"), top10=("top10", "sum"))
    if por_genero.empty:
        f3 = figura_vacia("3. Top 10 por género")
    else:
        por_genero["porcentaje"] = 100 * por_genero["top10"] / por_genero["total"]
        por_genero = por_genero.sort_values("porcentaje")
        por_genero["muestra"] = por_genero["total"].map(lambda x: f"n={x:,}" + ("*" if x < 30 else ""))
        f3 = px.bar(por_genero, x="porcentaje", y="genero", orientation="h",
                    text="muestra", custom_data=["total", "top10"],
                    title="3. % que llegó al Top 10 por género",
                    labels={"porcentaje": "% que llegó al Top 10", "genero": ""},
                    color="porcentaje", color_continuous_scale=[(0, TURQUESA), (0.5, VIOLETA), (1, FUCSIA)])
        f3.update_traces(textposition="outside", cliponaxis=False,
                         hovertemplate="%{y}<br>Top 10: %{x:.1f}%<br>Total: %{customdata[0]:,} canciones"
                                       "<br>Exitosas: %{customdata[1]:,}<br><i>Clic para filtrar</i><extra></extra>")
        f3.update_layout(coloraxis_showscale=False)
        f3.update_xaxes(range=[0, max(30, float(por_genero["porcentaje"].max()) * 1.25)])

        f3 = estilo_grafico(f3, 420, margen=dict(l=140, r=80, t=66, b=55))


    if por_seleccion:
        f4 = no_update
    else:
        p = base.dropna(subset=["energy", "danceability"]).copy()
        if p.empty:
            f4 = figura_vacia("4. Energía y bailabilidad")
        else:
            p["exito"] = p["top10"].map({True: "Alcanzó Top 10", False: "No alcanzó Top 10"})
            f4 = px.scatter(p, x="danceability", y="energy", color="exito",
                            color_discrete_map={"Alcanzó Top 10": FUCSIA, "No alcanzó Top 10": TURQUESA},
                            category_orders={"exito": ["No alcanzó Top 10", "Alcanzó Top 10"]},
                            custom_data=["song_id"],
                            hover_data={"song": True, "performer": True, "mejor_puesto": True,
                                        "danceability": ":.2f", "energy": ":.2f"},
                            opacity=0.25, render_mode="webgl", title="4. Energía frente a bailabilidad",
                            labels={"danceability": "Bailabilidad (0–1)", "energy": "Energía (0–1)", "exito": "",
                                    "song": "Canción", "performer": "Artista", "mejor_puesto": "Mejor puesto"})
            f4.update_traces(marker=dict(size=6))

            ref = p[p["song_id"] == id_cancion]
            if not ref.empty:
                r0 = ref.iloc[0]
                f4.add_trace(go.Scatter(
                    x=[r0["danceability"]], y=[r0["energy"]], mode="markers", name="Canción del gráfico 1",
                    marker=dict(symbol="star", size=18, color=NARANJA, line=dict(color="white", width=1.5)),
                    customdata=[[r0["song_id"]]], text=[f"{r0['song']} — {r0['performer']}"],
                    hovertemplate="<b>%{text}</b><br>Bailabilidad: %{x:.2f}<br>Energía: %{y:.2f}<extra></extra>"
                ))

            f4.update_layout(dragmode="select", uirevision=f"{anios}-{genero}",
                             selectionrevision=f"{anios}-{genero}-{id_cancion}")
            f4.update_xaxes(range=[0, 1]); f4.update_yaxes(range=[0, 1])
            f4 = estilo_grafico(f4, 420, margen=dict(l=55, r=22, t=100, b=55))

            f4.update_layout(legend=dict(orientation="h", y=1.01, yanchor="bottom", x=0, title_text=""))


    def comparar(col):
        v = d.dropna(subset=[col])
        return v.loc[v["top10"], col], v.loc[~v["top10"], col]

    b_si, b_no = comparar("danceability")
    e_si, e_no = comparar("energy")
    if len(b_si) and len(b_no):
        hallazgo = (f"En este segmento, {exito:.1f} % de las {n:,} canciones llegó al Top 10. "
                    f"Bailabilidad media: {b_si.mean():.3f} en las {len(b_si):,} canciones Top 10 frente a "
                    f"{b_no.mean():.3f} en las {len(b_no):,} restantes (diferencia {b_si.mean() - b_no.mean():+.3f}). "
                    f"Energía media: {e_si.mean():.3f} frente a {e_no.mean():.3f} "
                    f"(diferencia {e_si.mean() - e_no.mean():+.3f}). "
                    "Son comparaciones descriptivas, no relaciones causales.")
    else:
        hallazgo = "Este segmento no tiene suficientes canciones con audio en ambos grupos para comparar."

    return k1, k2, k3, estado, f1, f2, f3, f4, hallazgo, nueva_seleccion


if __name__ == "__main__":

    app.run(debug=False, dev_tools_ui=False)
