#!/bin/bash
# Print "NEW ALIAS" for every arm of the catalogue (the published fleet's name beside ours), for
# relabel.py's --map and for reading the comparison's alias lookups.
STUDY=${STUDY:-$(cd "$(dirname "$0")" && pwd)}
for a in $(awk '/^case \$arm in/{p=1; next} /^esac/{p=0} p && /^  [a-z0-9_]+\)/{sub(/\).*/, ""); sub(/^ +/, ""); print}' "$STUDY/arms.sh"); do
  echo "$a $(arm=$a seed=1 N=500 pay=p1m bash -c "source '$STUDY/arms.sh'; echo \$ALIAS")"
done
