INPUT_FILE=$1
DOT_FILE=$INPUT_FILE.dot
PNG_FILE=$INPUT_FILE.png

rm -rf $PNG_FILE $DOT_FILE

# dot file generation

echo "digraph G {" > $DOT_FILE

	while read line; do
		ps=($line)
		source=${ps[0]}
		targets=${ps[1]}
		targets=(${targets//,/ })
		for i in "${!targets[@]}"
		do
			target=${targets[i]}
			if [ $target != '==' ]; then
				echo "$source -> $target" >> $DOT_FILE
			else
				echo "$source" >> $DOT_FILE
			fi
		done
	done < "${INPUT_FILE}"

echo "}" >> $DOT_FILE

# rendering
dot -Tpng ${DOT_FILE} > $PNG_FILE

gnome-open $PNG_FILE
