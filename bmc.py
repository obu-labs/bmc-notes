#!/bin/python3

import argparse
from pathlib import Path
from urllib.parse import urljoin
from functools import cache
from collections import defaultdict
import os
import re

import bs4
import joblib
import requests
from markdownify import markdownify
from unidecode import unidecode

from vnmutils.mdutils import (
  abs_path_to_obsidian_link_text,
  SCUID_SEGMENT_PATHS,
)

TOC_URL = "https://www.dhammatalks.org/vinaya/bmc/Section0000.html"
TOC_URL_TO_SUBDIR = {
  './Section0001.html': 'vol1',
  "./Section0006.html": 'Introduction',
  "./Section0007.html": "About",
  "./Section0008.html": "Dependence",
  "./Section0028.html#appendixIX": "Thullaccayas",
  "./Section0034.html": 'vol2',
  "./Section0038.html": 'About',
  "./Section0052.html": "About",
}
TOC_URLS_TO_SKIP = {
  "./Section0000.html", # Main TOC: maybe do later?
  './Section0001.html',
  "./Cover.html",
  "./Section0003.html",
  "./Section0034.html",
  "./Section0013.html#NP_ChOne",
  "./Section0014.html",
  "./Section0015.html",
  "./Section0016.html#Pc_ChOne",
  "./Section0017.html",
  "./Section0018.html",
  "./Section0020.html",
  "./Section0021.html",
  "./Section0022.html",
  "./Section0023.html",
  "./Section0024.html",
  "./Section0026.html#Sk_ChTwo",
  "./Section0026.html#Sk_ChFour",
  "./Section0028.html",
  "./Section0028.html#appendixVIII",
  "./Section0030.html#sigil_toc_id_40",
  "./Section0033.html",
  "./Section0039.html",
  "./Section0051.html",
  "./Section0064.html",
  "./Section0067.html",
  "./Section0068.html",
  "./Section0072.html",
}
URL_TITLE_OVERRIDES = {
  "./Section0008.html": "Nissaya",
  "./Section0008.html#sigil_toc_id_6": "Nissaya Duties",
  "./Section0008.html#sigil_toc_id_9": "Dismissal of a Student",
  "./Section0009.html#sigil_toc_id_14": "Mindstate when Disrobing",
  "./Section0009.html#sigil_toc_id_15": "The Intent to Disrobe",
  "./Section0009.html#sigil_toc_id_16": "The Disrobing Statement",
  "./Section0009.html#sigil_toc_id_17": "The Witness to the Disrobal",
  "./Section0026.html#Sk_ChOne": "The Sekhiyas on Proper Behavior",
  "./Section0026.html#Sk_ChThree": "The Sekhiyas on Teaching",
  "./Section0028.html#sigil_toc_id_27": "Other Transaction Statements",
  "./Section0028.html#appendixIX": "Thullaccaya offenses",
  "./Section0028.html#appendixX": "A pupil’s duties as attendant to his mentor",
  "./Section0030.html": "Rule Index",
  "./Section0038.html#sigil_toc_id_49": "Format of the Khandhakas",
  "./Section0038.html#sigil_toc_id_50": "The Khandhaka Rules",
  "./Section0038.html#sigil_toc_id_51": "Methodology",
  "./Section0050.html#sigil_toc_id_140": "The Rains Determination",
  "./Section0050.html#sigil_toc_id_141": "The Duration of the Rains",
  "./Section0050.html#sigil_toc_id_143": "Rains Obstacles",
  "./Section0050.html#sigil_toc_id_146": "Rains Privileges",
  "./Section0052.html#sigil_toc_id_153": "Community Transaction Offenses",
  "./Section0054.html#sigil_toc_id_161": "The Ordination Candidate",
  "./Section0054.html#sigil_toc_id_162": "The Ordination Assembly",
  "./Section0054.html#sigil_toc_id_163": "The Ordination Statement",
  "./Section0055.html#sigil_toc_id_167": "The Uposatha Location",
  "./Section0055.html#sigil_toc_id_168": "Uposatha Unity",
  "./Section0055.html#sigil_toc_id_169": "Excluded from the Uposatha",
  "./Section0055.html#sigil_toc_id_170": "Uposatha Preliminaries",
  "./Section0055.html#sigil_toc_id_174": "A Purity Uposatha",
  "./Section0055.html#sigil_toc_id_175": "A Solitary Uposatha",
  "./Section0055.html#sigil_toc_id_176": "Borderline Uposathas",
  "./Section0055.html#sigil_toc_id_177": "Uposathas while Traveling",
  "./Section0055.html#sigil_toc_id_178": "Uposatha Unity Cases",
  "./Section0055.html#sigil_toc_id_179": "Uposatha Accusations",
  "./Section0056.html#sigil_toc_id_185": "Invitation Duties",
  "./Section0056.html#sigil_toc_id_186": "The Invitation Quorum",
  "./Section0056.html#sigil_toc_id_189": "A Solitary Invitation",
  "./Section0056.html#sigil_toc_id_190": "Borderline Invitations",
  "./Section0056.html#sigil_toc_id_191": "Invitational Accusations",
  "./Section0056.html#sigil_toc_id_192": "Two Invitation Groups",
  "./Section0056.html#sigil_toc_id_195": "Other Invitation Issues",
  "./Section0057.html#sigil_toc_id_198": "Kathina Time Period",
  "./Section0057.html#sigil_toc_id_199": "The Kathina Donor",
  "./Section0057.html#sigil_toc_id_200": "The Kathina Cloth",
  "./Section0057.html#sigil_toc_id_201": "The Kathina Transaction",
  "./Section0057.html#sigil_toc_id_202": "Making the Kathina Robe",
  "./Section0057.html#sigil_toc_id_203": "Spreading the Kathina Cloth",
  "./Section0057.html#sigil_toc_id_204": "The Kathina Privileges",
  "./Section0058.html#sigil_toc_id_208": "Qualifications for Community Officials",
  "./Section0058.html#sigil_toc_id_212": "Miscellaneous Dispensers",
  "./Section0059.html#sigil_toc_id_217": "The Formal Statements and Transactions for Penance and Probation",
  "./Section0059.html#sigil_toc_id_220": "Penance for multiple offenses",
  "./Section0059.html#sigil_toc_id_221": "Penance for shared offenses",
  "./Section0059.html#sigil_toc_id_222": "Penance or Probation Interruptions",
  "./Section0060.html#sigil_toc_id_226": "Disciplinary acts with the laity",
  "./Section0063.html#sigil_toc_id_235": "Inheriting belongings",
  "./Section0065.html#sigil_toc_id_239": "Communal relations with Bhikkhunis",
  "./Section0065.html#sigil_toc_id_240": "Individual relations with Bhikkhunis",
  "./Section0066.html#sigil_toc_id_243": "Training novices",
  "./Section0066.html#sigil_toc_id_244": "Novice Dependence",
  "./Section0066.html#sigil_toc_id_245": "Punishing Novices",
  "./Section0066.html#sigil_toc_id_246": "Expelling Novices",
  "./Section0070.html#sigil_toc_id_262": "Vuṭṭhāna-vidhī for one unconcealed offense",
  "./Section0070.html#sigil_toc_id_263": "Vuṭṭhāna-vidhī for one concealed offense",
  "./Section0070.html#sigil_toc_id_264": "The Transaction for Aggha-samodhāna-parivāsa (Combined Probation)",
  "./Section0070.html#sigil_toc_id_265": "Missaka-samodhāna-parivāsa (Mixed Combination for Offenses of Different Bases) Transaction",
  "./Section0070.html#sigil_toc_id_266": "Mūlāya paṭikassanā The Transaction for Sending Back to the Beginning",
  "./Section0070.html#sigil_toc_id_267": "The Transaction for Suddhanta-parivāsa (Purifying Probation)",
}

ENGLISH_DIGITS = ["One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"]

ROOT_FOLDER = Path(__file__).parent
CACHE_FOLDER = ROOT_FOLDER / ".cache"
SCID_MAP_FILE = ROOT_FOLDER / "scidmap.json"

disk_memoizer = joblib.Memory(
  CACHE_FOLDER,
  verbose=0,
)

@disk_memoizer.cache()
def cached_get(url: str) -> str:
  r = requests.get(url)
  return r.content.decode('utf-8')

def link_to_subdir_name(link: bs4.element.Tag) -> str:
  try:
    return TOC_URL_TO_SUBDIR[link['href']]
  except KeyError:
    pass
  ret = unidecode(link.text.strip())
  if ':' not in ret:
    return ret.replace("&", "and")
  ret = ret.split(':')
  try:
    digit = ENGLISH_DIGITS.index(ret[0]) + 1
  except ValueError:
    return ret[1].strip()
  ret = ret[1].replace(" Chapter", "").replace(" The ", "").strip()
  return f"Ch{digit}_ {ret}"

def link_to_note_title(link: bs4.element.Tag) -> str:
  assert link.name == 'a'
  try:
    return URL_TITLE_OVERRIDES[link['href']]
  except KeyError:
    pass
  ret = unidecode(link.text.strip())
  # The dual Sekhiyas
  if ret.endswith(']'):
    ret = ret[:-1]
    ret = ret.split(' [')
    return f"Sekhiya {ret[0]} and {ret[1]}"
  # BMCv1 Rule Index
  if link["href"].startswith("./Section0030.html"):
    return ret + " Rules"
  if ':' not in ret:
    return ret.replace("&", "and")
  ret = ret.split(':')
  return ret[1].strip()

def bmc_href_to_uri(href: str) -> str:
  if href.startswith("./"):
    return urljoin(TOC_URL, href)
  if href.startswith("http"):
    return href
  if href.startswith("/"):
    return f"https://www.dhammatalks.org{href}"
  if href.startswith("#"):
    raise Exception("Please handle in-page links upstream!")
  raise Exception(f"Unknown href type: {href}")

internal_link_map = {} # build_internal_link_map to init

def _build_plan_from_toc(toc_element: bs4.element.Tag, cwd: Path) -> list[tuple[Path, str]]:
  """A function for building the document plan from the parsed Table of Contents html."""
  prefix = 'BMCv1'
  rule_category = None
  ret = []
  while toc_element:
    assert toc_element.name == 'li'
    link = toc_element.next_element
    assert link.name == 'a'
    url = link['href']
    is_patimokkha_rule = False
    if url >= "./Section0010.html" and url < "./Section0028.html":
      is_patimokkha_rule = True
      if not ("#" in url or ":" in link.text):
        rule_category = unidecode(link.text)
    nextelement = link.next_element
    nextelement = nextelement.next_element
    assert nextelement.text == '\n'
    nextelement = nextelement.next_element
    if nextelement.name == 'ul':
      subdir = link_to_subdir_name(link)
      cwd = cwd.joinpath(subdir)
      cwd.mkdir(exist_ok=True)
      if subdir == 'vol2':
        prefix = 'BMCv2'
    if url not in TOC_URLS_TO_SKIP and not (prefix == "BMCv2" and link.text == "Discussions" and link["href"] != "./Section0038.html#sigil_toc_id_51"):
      if is_patimokkha_rule:
        if link.text.isdigit():
          ret.append((cwd.joinpath(f"{prefix} {rule_category} {link.text}.md"), url))
        else:
          suffix = ''
          if "#" not in url:
            suffix = " Introduction"
          ret.append((cwd.joinpath(f"{prefix} {link_to_note_title(link)}{suffix}.md"), url))
      elif prefix == "BMCv2" and link.text == "Rules":
        name = cwd.name
        if link['href'] == "./Section0052.html#sigil_toc_id_154":
          name = "Community Transaction"
        elif link['href'] == "./Section0038.html#sigil_toc_id_50":
          name = "The Khandhaka"
        ret.append((cwd.joinpath(f"{prefix} {name} Rules.md"), url))
      elif prefix == "BMCv2" and cwd.name == "General Transaction Statements":
        ret.append((cwd.joinpath(f"{prefix} Transaction Statements Regarding {link.text[3:].replace('&', 'and')}.md"), url))
      elif prefix == "BMCv2" and cwd.name == "Going-forth and Acceptance":
        ret.append((cwd.joinpath(f"{prefix} Transaction Statements for {link.text[3:].replace(':', '')}.md"), url))
      elif prefix == "BMCv2" and cwd.name == "Disciplinary Transactions":
        ret.append((cwd.joinpath(f"{prefix} Transaction Statements for {link.text[3:]}.md"), url))
      elif prefix == "BMCv2" and cwd.name == "Technical Terms":
        technical_term = link.text
        # fill in the translation for this one too
        if link.text == "D. Anāmāsa":
          technical_term = "D. Anāmāsa: Not to be Touched"
        ret.append((cwd.joinpath(f"{prefix} {technical_term[3:].replace('&', 'and').replace(': ', ' (')}).md"), url))
      else:
        ret.append((cwd.joinpath(f"{prefix} {link_to_note_title(link)}.md"), url))
    uplevels = 0
    while nextelement.text == '\n':
      nextelement = nextelement.next_element
      uplevels += 1
    if isinstance(nextelement, bs4.Comment):
      # This comment is what ends the table of contents
      if nextelement != "end contents":
        raise Exception(f"Expected \"end contents\" got \"{nextelement}\"")
      return ret
    match nextelement.name:
      case 'ul': # going down into a sublist
        toc_element = nextelement.next_element
        assert toc_element.text == '\n'
        toc_element = toc_element.next_element
      case 'li': # possibly jumping out of a sublist
        curul = toc_element.find_parent('ul')
        nextul = nextelement.find_parent('ul')
        jumpcount = 0
        while curul != nextul:
          cwd = cwd.parent
          curul = curul.find_parent('ul')
          jumpcount += 1
          if jumpcount > 3 or not curul:
            raise Exception(f"Failed to figure out level of next element: {nextelement}")
        toc_element = nextelement
        # assert that we jumped up the same number of levels
        # that our '\n's expected us to jump
        assert jumpcount == uplevels
      case _:
        raise Exception(f"Unexpected next element: {nextelement} with name {nextelement.name}")
  raise Exception("ERROR: Unexpected end to table of contents html")

def forumlate_plan() -> list[tuple[Path, str]]:
  """Returns a list of planned documents to generate.
  
  Fetches and parses the Table of Contents into:
    (path, url)
  """
  toc_html = cached_get(TOC_URL)
  soup = bs4.BeautifulSoup(toc_html, 'html.parser')
  contents = soup.find(id="contents")
  toc = contents.find_next('li')
  return _build_plan_from_toc(toc, BMC_FOLDER)

@cache
def cached_soup(href: str) -> bs4.element.Tag:
  return bs4.BeautifulSoup(
    cached_get(bmc_href_to_uri(href)),
    'html.parser',
  ).find(id="vinaya")

@cache
def cooked_soup(href: str) -> bs4.element.Tag:
  soup = cached_soup(href)
  for linkelem in soup.find_all('a', attrs={'href': True}):
    ahref = linkelem['href']
    if ahref[0] == "#":
      ahref = href + ahref
    if ahref in internal_link_map:
      linkelem['href'] = internal_link_map[ahref]
    else:
      linkelem['href'] = bmc_href_to_uri(ahref)
  return soup

def first_sibling(element: bs4.element.Tag) -> bs4.element.Tag:
  ret = element
  while ret.find_previous_sibling():
    ret = ret.find_previous_sibling()
  return ret

def last_sibling(element: bs4.element.Tag) -> bs4.element.Tag:
  ret = element
  while ret.find_next_sibling():
    ret = ret.find_next_sibling()
  return ret

def html_range(soup: bs4.element.Tag, relative_to: Path, startid=None, endid=None) -> str:
  def renderhtml(element: bs4.element.Tag) -> str:
    if not isinstance(element, bs4.NavigableString):
      for linkelem in element.find_all('a', attrs={'href': True}):
        if linkelem['href'].startswith(str(ROOT_FOLDER)):
          linkelem['href'] = os.path.relpath(linkelem['href'], relative_to).replace(' ', '%20')
    return str(element)
  if not (startid or endid):
    return str(soup)
  collected_html = []
  include_last = False
  if startid:
    startid = soup.find(id=startid)
  if endid:
    endid = soup.find(id=endid)
  if not startid:
    startid = first_sibling(endid)
  if not endid:
    include_last = True
    endid = last_sibling(startid)
  if startid.parent != endid.parent:
    if endid.text == "Rules":
      include_last = True
      endid = last_sibling(startid)
    else:
      raise Exception(f"{str(startid)} and {str(endid)} are not siblings")
  while startid != endid:
    if not isinstance(startid, bs4.Comment):
      collected_html.append(renderhtml(startid))
    startid = startid.next_sibling
  if include_last:
    collected_html.append(renderhtml(endid))
  return ''.join(collected_html)

def get_rule_link(href: str) -> Path | None:
  """Returns the abs path to the rule file this href is about if any"""
  m = re.match(r'\./Section(\d+)\.html?#(Pr|Sg|Ay|NP|Pc|Pd|Sk)(\d+)', href)
  if not m:
    return None
  sec_num = int(m.group(1))
  rule_type = m.group(2)
  rule_num = int(m.group(3))
  scref = "pli-tv-bu-pm-"
  match rule_type:
    case 'Pr':
      assert sec_num == 10
      scref += f"pj{rule_num}"
    case 'Sg':
      assert sec_num == 11
      scref += f"ss{rule_num}"
    case 'Ay':
      assert sec_num == 12
      scref += f"ay{rule_num}"
    case 'NP':
      assert sec_num >= 13 and sec_num <= 15
      scref += f"np{rule_num}"
    case 'Pc':
      assert sec_num >= 16 and sec_num <= 24
      scref += f"pc{rule_num}"
    case 'Pd':
      assert sec_num == 25
      scref += f"pd{rule_num}"
    case 'Sk':
      assert sec_num == 26
      scref += f"sk{rule_num}"
    case _:
      raise Exception(f"Unknown rule type {rule_type}")
  return SCUID_SEGMENT_PATHS.get(scref)

def write_dhammatalks_html_to_md_file(html_content: str, plan: tuple[Path, str], next_path: Path | None = None) -> None:
  md = markdownify(html_content)
  regarding_link = get_rule_link(plan[1])
  if regarding_link:
    regarding_link = f'About: [{regarding_link.stem}{abs_path_to_obsidian_link_text(regarding_link, plan[0])}'
  else:
    regarding_link = ''
  next_path_md = ""
  if next_path:
    next_path_md = f"""
**Next Section: [{next_path.stem}{abs_path_to_obsidian_link_text(next_path, plan[0].parent)}**
"""
  else:
    next_path_md = "\n***Fin***"
  plan[0].write_text(f"""
Source: <{bmc_href_to_uri(plan[1])}>  
{regarding_link}
{md}
{next_path_md}""")

def build_internal_link_map(plan: list[tuple[Path, str]]) -> dict[str, str]:
  """Maps hrefs you may find in the page to where they should link"""
  if len(internal_link_map) > 0:
    raise Exception("Don't build internal_link_map multiple times plz")
  plan_for_each_page = defaultdict(dict)
  for doc in plan:
    s = doc[1].split("#")
    anchor = s[1] if len(s) > 1 else 'vinaya'
    plan_for_each_page[s[0]][anchor] = str(doc[0])
  for href, doc_for_id in plan_for_each_page.items():
    soup = cached_soup(href)
    curlink = doc_for_id.get('vinaya', bmc_href_to_uri(href))
    internal_link_map[href] = curlink
    for elem in soup.find_all(attrs={'id': True}):
      if elem['id'] in doc_for_id:
        curlink = doc_for_id[elem['id']]
      internal_link_map[f"{href}#{elem['id']}"] = curlink

def execute_plan(plan: list[tuple[Path, str]], current_document_index: int) -> None:
  baseurl = plan[current_document_index][1].split("#")[0]
  html_content = cooked_soup(baseurl)
  curid = None
  nextid = None
  if "#" in plan[current_document_index][1]:
    curid = plan[current_document_index][1].split("#")[1]
  if current_document_index+1 < len(plan) and plan[current_document_index+1][1].startswith(baseurl):
    assert "#" in plan[current_document_index+1][1]
    nextid = plan[current_document_index+1][1].split("#")[1]
  write_dhammatalks_html_to_md_file(
    html_range(html_content, plan[current_document_index][0].parent, startid=curid, endid=nextid),
    plan[current_document_index],
    next_path=plan[current_document_index+1][0] if current_document_index+1 < len(plan) else None,
  )

if __name__ == "__main__":
  arg_parser = argparse.ArgumentParser(
    description="Downloads BMCv1-2 from Dhammatalks.org as a set of Markdown files.",
  )
  arg_parser.add_argument(
    'outputdir',
    type=Path,
    help="Output Directory",
    default=ROOT_FOLDER / "The BMC",
    nargs='?',
  )
  args = arg_parser.parse_args()
  global BMC_FOLDER
  BMC_FOLDER = args.outputdir
  if BMC_FOLDER.exists():
    print(f"{BMC_FOLDER} already exists. rm -rf {BMC_FOLDER} and try again.")
    exit(1)
  BMC_FOLDER.mkdir()
  SCUID_SEGMENT_PATHS.load_data_from_json(
    SCID_MAP_FILE.read_text(),
    BMC_FOLDER.parent,
  )
  plan = forumlate_plan()
  build_internal_link_map(plan)
  for i in range(len(plan)):
    execute_plan(plan, i)
