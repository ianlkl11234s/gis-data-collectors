import io,json
from pathlib import Path
import pytest
from tasks.jp_medical_artifacts import BundlePlan,build_plan,sha256_bytes
from tasks.jp_medical_publish import publish_reviewed_payload
class Fake:
 def __init__(self,corrupt=False):self.objects={};self.keys=[];self.corrupt=corrupt
 def put_object(self,**kw):self.objects[kw['Key']]=kw;self.keys.append(kw['Key'])
 def get_object(self,**kw):
  v=self.objects[kw['Key']];return {'Body':io.BytesIO(b'bad' if self.corrupt else v['Body']),'CacheControl':v['CacheControl']}
def fixture(tmp):
 raw=tmp/'raw';pub=tmp/'pub';raw.mkdir();pub.mkdir();(raw/'source.csv').write_bytes(b'one,two\n');(pub/'public-index.json').write_text('{}')
 inventory=[{'path':'public-index.json','sha256':sha256_bytes((pub/'public-index.json').read_bytes()),'bytes':2}]
 digest=sha256_bytes(json.dumps({'dataset':'jp_medical_navii','files':inventory},ensure_ascii=False,sort_keys=True,separators=(',',':')).encode())
 p=BundlePlan('jp_medical_navii',tmp,pub,raw,pub,digest,((pub/'public-index.json',Path('public-index.json')),),(raw/'source.csv',))
 manifest=tmp/'approved.json';manifest.write_text(json.dumps({'dataset':p.dataset,'raw':[{'path':'source.csv','sha256':sha256_bytes((raw/'source.csv').read_bytes()),'bytes':8}]}));return p,manifest
def frontend_fixture(tmp):
 root=tmp/'analytics';release=root/'data/processed/world/jp_medical_frontend/releases/r1'
 jsonl=release/'care/shards/01.jsonl';pmtiles=release/'areas/a.pmtiles';extra=release/'unlisted.geojson'
 jsonl.parent.mkdir(parents=True);pmtiles.parent.mkdir(parents=True)
 jsonl.write_bytes(b'{"id":1}\n');pmtiles.write_bytes(b'pmtiles');extra.write_text('{}')
 files={path.relative_to(release).as_posix():{'sha256':sha256_bytes(path.read_bytes()),'bytes':path.stat().st_size} for path in (jsonl,pmtiles)}
 (release/'catalog.json').write_text(json.dumps({'files':files}))
 current=root/'data/processed/world/jp_medical_frontend/current.json';current.parent.mkdir(parents=True,exist_ok=True)
 current.write_text(json.dumps({'catalog':'releases/r1/catalog.json'}))
 manifest=tmp/'frontend-approved.json';manifest.write_text(json.dumps({'dataset':'jp_medical_frontend','raw':[]}))
 return build_plan('jp_medical_frontend',root),manifest
def test_readback_before_current(tmp_path):
 p,m=fixture(tmp_path);f=Fake();r=publish_reviewed_payload(f,bucket='explicit-test-bucket',plan=p,payload_manifest=m)
 assert f.keys[-1].endswith('/current.json');assert len(r['receipts'])==3
 assert f.objects[f.keys[-1]]['CacheControl']=='public,max-age=60'
def test_failed_archive_never_promotes_current(tmp_path):
 p,m=fixture(tmp_path);f=Fake(corrupt=True)
 with pytest.raises(RuntimeError,match='readback'):publish_reviewed_payload(f,bucket='explicit-test-bucket',plan=p,payload_manifest=m)
 assert not any(k.endswith('/current.json') for k in f.keys)
def test_changed_payload_prevents_all_writes(tmp_path):
 p,m=fixture(tmp_path);(p.raw_root/'source.csv').write_bytes(b'changed');f=Fake()
 with pytest.raises(ValueError):publish_reviewed_payload(f,bucket='explicit-test-bucket',plan=p,payload_manifest=m)
 assert f.keys==[]
def test_frontend_catalog_allowlist_uploads_mixed_artifacts_only(tmp_path):
 p,m=frontend_fixture(tmp_path);f=Fake();publish_reviewed_payload(f,bucket='explicit-test-bucket',plan=p,payload_manifest=m)
 paths={relative.as_posix() for _,relative in p.artifacts}
 assert paths=={'catalog.json','care/shards/01.jsonl','areas/a.pmtiles'}
 assert not any('unlisted.geojson' in key for key in f.keys)
 assert next(v for k,v in f.objects.items() if k.endswith('/care/shards/01.jsonl'))['ContentType']=='application/x-ndjson'
 assert next(v for k,v in f.objects.items() if k.endswith('/areas/a.pmtiles'))['ContentType']=='application/vnd.pmtiles'
 current=json.loads(f.objects[f.keys[-1]]['Body'])
 assert current['version']==p.bundle_hash
 assert current['catalog']==f'releases/{p.bundle_hash}/catalog.json'
 assert current['files']['catalog.json']['key'].endswith(current['catalog'])
