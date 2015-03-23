import java.util.Iterator;
import java.util.Random;

public class TreeRLEList <T> implements RLEList<T>, Iterable<T> {
	private Node head;
	
	@Override
	public void append(T value) {
		insert(head == null ? 0 : head.getTotalCount(), value);
	}

	@Override
	public void insert(int index, T value) {
		if (head == null) {
			head = insertToTree(head, index, value);	
			return;
		}
		if (head != null && index == head.getTotalCount()) {
			Node lastNode = getKth(head, head.getTotalCount());
			if (lastNode.getValue().equals(value)) {
				lastNode.setValueCount(lastNode.getValueCount() + 1);
				lastNode.updateParent();
			}
			else {
				head = insertToTree(head, index, value);				
			}
			return;
		}
		if (head != null && index == 0) {
			Node firstNode = getKth(head, 1);
			if (firstNode.getValue().equals(value)) {
				firstNode.setValueCount(firstNode.getValueCount() + 1);
				firstNode.updateParent();
			}
			else {
				head = insertToTree(head, index, value);				
			}
			return;			
		}
		Node curNode = getKth(head, index + 1); 
		if (curNode.getValue().equals(value)) {
			curNode.setValueCount(curNode.getValueCount() + 1);
			curNode.updateParent();	
		}
		else {
			if (getKth(head, index).getValue().equals(curNode.getValue())) {
				head = addAndSplit(head, index + 1, value);
			}
			else {
				head = insertToTree(head, index, value);	
			}
		}
	}

	@Override
	public T get(int index) {
		return getKth(head, index + 1).getValue();
	}

	@Override
	public Iterator<T> iterator() {
		return new TreeRLEListIterator();
	}
	
	Random random = new Random();
	class Node {
		private Node left;
		private Node right;
		private Node parent;
		private T value;
		private int weight;
		private int valueCount;
		private int totalCount;

		Node(T value) {
			this.value = value;
			this.valueCount = totalCount = 1;
			this.weight = random.nextInt();
		}

		Node getLeft() {
			return left;
		}

		Node getRight() {
			return right;
		}
		
		void setParent(Node parent) {
			this.parent = parent;
		}
		
		void setLeft(Node left) {
			this.left = left;
		}
		
		void setRight(Node right) {
			this.right = right;
		}
		
		T getValue() {
			return value;
		}
		
		int getWeight() {
			return weight;
		}
		
		int getTotalCount() {
			return totalCount;
		}
		
		void setValueCount(int valueCount) {
			this.valueCount = valueCount;
			updateParent();
		}
		
		int getValueCount() {
			return valueCount;
		}
		
		void recalc() {
			totalCount = valueCount;
			if (left != null) {
				totalCount += left.getTotalCount();
				left.setParent(this);
			}
			if (right != null) {
				totalCount += right.getTotalCount();
				right.setParent(this);
			}
		}
		
		void updateParent() {
			recalc();
			if (parent != null)
				parent.updateParent();
		}
	}
	private Node merge(Node left, Node right) {
	    if (left == null)
	    	return right;
	    else
	    	if (right == null)
	    		return left;
	    if (left.getWeight() > right.getWeight()) {
	    	left.setRight(merge(left.getRight(), right));
	    	left.recalc();
	    	return left;
	    }
	    else {
	    	right.setLeft(merge(left, right.getLeft()));
	    	right.recalc();
	    	return right;
	    }
	}
	
	int left(Node tree) {
	    if (tree == null) 
	    	return 0;
	    int x = 1;
	    if (tree.getLeft() != null) 
	    	x += tree.getLeft().getTotalCount();
	    return x;
	}
	
	Node getKth(Node tree, int k) {
		if (k == 0)
			return tree;
		int leftCnt = tree.getLeft() != null ? tree.getLeft().getTotalCount() : 0;
	    if (leftCnt + tree.getValueCount() >= k && leftCnt + 1 <= k)
			return tree;
		else {
			if (leftCnt >= k)
				return getKth(tree.getLeft(), k);
			else
				return getKth(tree.getRight(), k - leftCnt - tree.getValueCount());
		}
	}
	
	Node addAndSplit(Node tree, int k, T value) {
		if (k == 0) {
			//çäåñü ôèãà÷èì
		}
		int leftCnt = tree.getLeft() != null ? tree.getLeft().getTotalCount() : 0;
	    if (leftCnt + tree.getValueCount() >= k && leftCnt + 1 <= k) {
	    	//partOne
	    	int partOneValue = k - leftCnt - 1; // 0 íå ìîæåò áûòü, èíà÷å îçíà÷àëî áû ÷òî ñëåâà òàêîé æå
	    	int partTwoValue = leftCnt + tree.getValueCount() - k + 1;
		    Node m = new Node(value);
		    Node ff = new Node(tree.getValue());
		    Node ss = new Node(tree.getValue());
		    ff.setValueCount(partOneValue);
		    ff.updateParent();
		    ss.setValueCount(partTwoValue);
		    ss.updateParent();
		    ff.setLeft(tree.getLeft());
		    ff.updateParent();
		    ss.setRight(tree.getRight());
		    ss.updateParent();
//		    m = merge(m, ff);
		    m.setLeft(ff);
		    m.updateParent();
		    m.setRight(ss);
		    m.updateParent();
	//	    m = merge(m, ss);
		    return m;
	    }
		else {
			if (leftCnt >= k)
				tree.setLeft(addAndSplit(tree.getLeft(), k, value));
			else
				tree.setRight(addAndSplit(tree.getRight(), k - leftCnt - tree.getValueCount(), value));
		}		
	    tree.updateParent();
	    return tree;
	}
	
	private class NodesLeftRight {
		Node left;
		Node right;
		public NodesLeftRight() {
			left = right = null;
		}
		void setLeft(Node left) {
			this.left = left;
		}
		void setRight(Node right) {
			this.right = right;
		}
		Node getLeft() {
			return left;
		}
		Node getRight() {
			return right;
		}
		void recalc() {
			if (left != null)
				left.recalc();
			if (right != null)
				right.recalc();
		}
	}
	NodesLeftRight split(Node tree, int key) {
		NodesLeftRight leftRight = new NodesLeftRight();
	    if (tree == null)
	        return leftRight;
		if (left(tree) <= key) {
			NodesLeftRight splitLeftRight = split(tree.getRight(), key - left(tree));
			tree.setRight(splitLeftRight.getLeft());
			leftRight.setLeft(tree);
			leftRight.setRight(splitLeftRight.getRight());
		} else {
			NodesLeftRight splitLeftRight = split(tree.getLeft(), key);
			tree.setLeft(splitLeftRight.getRight());
			leftRight.setLeft(splitLeftRight.getLeft());
			leftRight.setRight(tree);
		}
		leftRight.recalc();	
		tree.recalc();
		return leftRight;
	}
	Node insertToTree(Node tree, int index, T value) {
	    Node m = new Node(value);
	    NodesLeftRight leftRight = split(tree, index);
	    leftRight.setLeft(merge(leftRight.getLeft(), m));
	    return merge(leftRight.getLeft(), leftRight.getRight());
	}
	
	private class TreeRLEListIterator implements Iterator<T> {
		private int size;
		private int repeat;
		private int needRepeat;
		private int currentIteratorNum;
		private Node currentNode;
		
		Node getFirstElement(Node currentNode) {
			while (currentNode.getLeft() != null)
				currentNode = currentNode.getLeft();
			return currentNode;
		}
		public TreeRLEListIterator() {
			size = head.getTotalCount();
			currentNode = null;
			currentIteratorNum = 0;
			currentNode = getFirstElement(head);
			repeat = -1;
		}
		
		@Override
		public boolean hasNext() {
			return currentIteratorNum < size;
		}

		@Override
		public T next() {
			if (repeat == -1 || repeat >= needRepeat) {
				currentNode = getKth(head, currentIteratorNum + 1);
				repeat = 0;
				needRepeat = currentNode.valueCount;
			}
			repeat++;
			currentIteratorNum++;
			return currentNode.getValue();
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
}

