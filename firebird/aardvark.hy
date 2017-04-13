(import json)
(import requests)

(defn pyccd-spectral-map
  ; returns map of pyccd spectra to ubid query
  ; params:
  ;   url - Full url for tile-spec endpoint
  [url]
  {:red     (.join "" [url "?q=tags:red AND sr"])
   :green   (.join "" [url "?q=tags:green AND sr"])
   :blue    (.join "" [url "?q=tags:blue AND sr"])
   :nir     (.join "" [url "?q=tags:nir AND sr"])
   :swir1   (.join "" [url "?q=tags:swir1 AND sr"])
   :swir2   (.join "" [url "?q=tags:swir2 AND sr"])
   :thermal (.join "" [url "?q=tags:thermal AND toa"])
   :cfmask  (.join "" [url "?q=tags:cfmask AND sr"])})

(defn ubids
  [query]
  (.get "ubid" (.loads json (. (.get requests query) text)))

(defn pyccd-inputs
  [polygon start_date end_date]
  ())
; this worked from the repl
; (.keys (.loads json (. (.get requests "https://jsonplaceholder.typicode.com/posts/1") text)))


;def tile_spec(url):
;    """ Returns the tile_spec from aardvark """
;    pass


;def snap(point, tile_spec):
;    """ Snaps a point to a chip id (tile_x, tile_y) """
;    pass


;def chip_ids(bbox, tile_spec):
;    """ Returns all the chip ids (tile_x, tile_y points) needed to
;        cover the bbox """
;    pass
