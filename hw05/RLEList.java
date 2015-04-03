import java.util.ArrayList;
import java.util.Iterator;

public interface RLEList<T> {
	void append(T value);
	void insert(int index, T value);
	T get(int index);
	Iterator<T> iterator();

	class Demo {
		public static void main(String[] args) {
	//		RLEList<String> list = new RLEListRefImpl<>();
			RLEList<String> list = new TreeRLEList<>();	
			
			
			//1
			/*list.append("car");
			list.append("cat");
			list.insert(2, "meet");
			list.insert(3, "kit");
			list.insert(2, "camel");
			list.append("camel");
			list.append("aaa");
			list.append("aaa");
			list.append("aaa");
			list.insert(0, "car");
			list.insert(0, "bbb");*/
			
			//!
			/*result 
			bbb
			car
			car
			cat
			camel
			meet
			kit
			camel
			aaa
			aaa
			aaa*/

			//2
		/*	list.append("aaa");
			list.append("bbb");
			list.append("aaa");
			list.append("aaa");
			list.insert(1, "bbb");
			list.insert(3, "aaa");
			
			//!
			/* result 
			 * aaa 
			 * bbb 
			 * bbb 
			 * aaa 
			 * aaa 
			 * aaa
			 */
			
			
			//3
		/*	list.append("aaa");
			list.append("bbb");
			list.append("bbb");
			list.append("ccc");
			list.insert(1, "ddd");*/
			/*
			 * result
			 * aaa
			 * ddd 
			 * bbb
			 * bbb
			 * ccc
			 * */
		
			//4
			list.append("aaa");
			list.append("bbb");
			list.append("bbb");
			list.append("ccc");
			list.insert(2, "ddd");
			/*
			 * result
			 * aaa
			 * bbb
			 * ddd
			 * bbb
			 * ccc
			 * */
			
			Iterator<String> it = list.iterator();
			while (it.hasNext())
				System.out.println(it.next());
		/*	System.out.println("--//--");
			for (int i = 0; i < 6; i++)
				System.out.println(list.get(i));*/
		}
	}
}

class RLEListRefImpl<T> implements RLEList<T> {
	private final ArrayList<T> myImpl = new ArrayList<>();

	public void append(T value) {
		myImpl.add(value);
	}

	public void insert(int index, T value) {
		myImpl.add(index, value);
	}

	public T get(int index) {
		return myImpl.get(index);
	}

	public Iterator<T> iterator() {
		return myImpl.iterator();
	}

}