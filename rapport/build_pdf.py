"""Génère rapport/rapport.pdf à partir de rapport/rapport.md.

Dépendances (hors Spark) : pip install markdown pymupdf
Usage : python rapport/build_pdf.py
"""
import os
import re

import markdown
import pymupdf

ICI = os.path.dirname(os.path.abspath(__file__))
CAPTURES = os.path.join(ICI, "..", "captures")

texte = open(os.path.join(ICI, "rapport.md"), encoding="utf-8").read()
entete, corps = re.match(r"---\n(.*?)\n---\n(.*)", texte, re.S).groups()
meta = dict(re.findall(r'(\w+):\s*"(.*)"', entete))

corps = corps.replace("../captures/", "")
html_corps = markdown.markdown(corps, extensions=["tables", "fenced_code"])
# légende sous chaque image (texte alternatif)


def figure(m):
    alt, src = m.group(1), m.group(2)
    pix = pymupdf.Pixmap(os.path.join(CAPTURES, src))
    # captures lisibles mais compactes : largeur max 430 pt, hauteur max 220 pt
    largeur_pt = int(min(430, pix.width * 0.28, 220 * pix.width / pix.height))
    return f'<p class="fig"><img src="{src}" width="{largeur_pt}"/><br/><i>Figure : {alt}</i></p>'


html_corps = re.sub(r'<p><img alt="([^"]*)" src="([^"]*)" ?/?></p>', figure, html_corps)

css = """
body { font-family: sans-serif; font-size: 9pt; line-height: 1.2; }
h1 { font-size: 15pt; color: #1f3a5f; margin-top: 10pt; border-bottom: 1px solid #1f3a5f; }
h2 { font-size: 12pt; color: #1f3a5f; margin-top: 10pt; }
table { border-collapse: collapse; margin: 6pt 0; }
th, td { border: 1px solid #999; padding: 2pt 5pt; font-size: 9pt; }
th { font-weight: bold; }
code { font-family: monospace; font-size: 9pt; }
pre { font-family: monospace; font-size: 8.5pt; background-color: #f4f4f4; padding: 4pt; }
p { margin-top: 3pt; margin-bottom: 3pt; }
p.fig { text-align: center; font-size: 8.5pt; color: #444; }
.titre { text-align: center; }
"""
page_titre = f"""
<div class="titre">
<p style="font-size:8pt">&nbsp;</p>
<h1 style="border:none; font-size:20pt">{meta['title']}</h1>
<p style="font-size:12pt">{meta['subtitle']}</p>
<p style="font-size:11pt">{meta['author']}</p>
<p>{meta['date']}</p>
<p>Code : https://github.com/abidhiaf/spark-reseau-trajets</p>
</div>
"""


def envelopper(fragment):
    return f"<html><head><style>{css}</style></head><body>{fragment}</body></html>"


# Découpage : texte et figures alternés. Une figure qui ne tient pas en bas de page passe
# à la page suivante (sinon PyMuPDF la réduirait pour la faire rentrer).
morceaux = re.split(r'(<p class="fig">.*?</p>)', page_titre + html_corps, flags=re.S)

archive = pymupdf.Archive(CAPTURES)
PAGE = pymupdf.paper_rect("a4")
X0, Y0, X1, Y1 = 42, 36, PAGE.width - 42, PAGE.height - 40
sortie = os.path.join(ICI, "rapport.pdf")
writer = pymupdf.DocumentWriter(sortie)
etat = {"dev": writer.begin_page(PAGE), "y": Y0}


def nouvelle_page():
    writer.end_page()
    etat["dev"] = writer.begin_page(PAGE)
    etat["y"] = Y0


for morceau in morceaux:
    if not morceau.strip():
        continue
    if morceau.startswith('<p class="fig">'):
        src = re.search(r'src="([^"]+)"', morceau).group(1)
        largeur = int(re.search(r'width="(\d+)"', morceau).group(1))
        pix = pymupdf.Pixmap(os.path.join(CAPTURES, src))
        besoin = largeur * pix.height / pix.width + 22          # image + légende
        if etat["y"] + besoin > Y1:
            nouvelle_page()
    story = pymupdf.Story(html=envelopper(morceau), archive=archive)
    while True:
        plus, rempli = story.place(pymupdf.Rect(X0, etat["y"], X1, Y1))
        story.draw(etat["dev"])
        etat["y"] = pymupdf.Rect(rempli).y1 + 2
        if not plus:
            break
        nouvelle_page()
writer.end_page()
writer.close()

doc = pymupdf.open(sortie)
for i, page in enumerate(doc, 1):
    if i > 1:
        page.insert_text((PAGE.width / 2 - 10, PAGE.height - 25), f"{i}", fontsize=8)
nb_pages = doc.page_count
doc.saveIncr()
print(f"PDF généré : {sortie} ({nb_pages} pages)")
