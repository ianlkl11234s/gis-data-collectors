import gzip, json, pytest
from collections import Counter
import scripts.gfw_v4_spatial_frames as frames
from scripts.gfw_v4_spatial_frames import build_spatial_frame
def test_frame_metadata_and_pmtiles_contract(tmp_path):
 s=tmp_path/'in.json.gz'; s.write_bytes(gzip.compress(json.dumps({'features':[{'type':'Feature','properties':{'vessel_id':'v-1','track_id':'frame-1'}}]}).encode(),mtime=0)); o=tmp_path/'releases/x/frames/a.pmtiles'; calls=[]
 def fake(**kw):
  calls.append(kw); kw['output'].parent.mkdir(parents=True,exist_ok=True); kw['output'].write_bytes(b'pmtiles')
 m=build_spatial_frame(source=s,output=o,observed_at='2026-08-21T00:00:00Z',bucket='fishing',release_root=tmp_path,pmtiles_builder=fake)
 assert calls[0]['named_inputs'][0][0]=='gfw_v4_track_frame' and m['path']=='releases/x/frames/a.pmtiles'
 assert m['content_encoding']=='identity' and m['semantic_counts']=={'observed_at':'2026-08-21T00:00:00Z','bucket':'fishing','feature_count':1}
 assert calls[0]['minimum_zoom']==calls[0]['maximum_zoom']==6 and m['spatial_contract']['identity_missing_count']==0

def test_spatial_builder_receives_stable_source_identities(tmp_path,monkeypatch):
 s=tmp_path/'in.json.gz'; s.write_bytes(gzip.compress(json.dumps({'features':[{'type':'Feature','properties':{'vessel_id':'v-1','track_id':'a'}},{'type':'Feature','properties':{'vessel_id':'v-2','track_id':'b'}}]}).encode(),mtime=0)); o=tmp_path/'frame.pmtiles'; calls=[]
 def fake(**kw):
  calls.append(kw); kw['output'].write_bytes(b'pmtiles')
 monkeypatch.setattr(frames,'_spatial_pmtiles',fake)
 build_spatial_frame(source=s,output=o,observed_at='2026-08-21T00:00:00Z',bucket='cargo',release_root=tmp_path,pmtiles_builder=fake)
 assert calls[0]['expected_identities']==Counter({'v-1:a':1,'v-2:b':1})

def test_duplicate_or_missing_identity_fails_closed(tmp_path):
 source=tmp_path/'in.json.gz'; output=tmp_path/'out.pmtiles'
 source.write_bytes(gzip.compress(json.dumps({'features':[{'type':'Feature','properties':{'vessel_id':'v','track_id':'t'}},{'type':'Feature','properties':{'vessel_id':'v','track_id':'t'}}]}).encode(),mtime=0))
 with pytest.raises(ValueError,match='duplicate'):
  build_spatial_frame(source=source,output=output,observed_at='2026-08-21T00:00:00Z',bucket='cargo',release_root=tmp_path,pmtiles_builder=lambda **_: None)
