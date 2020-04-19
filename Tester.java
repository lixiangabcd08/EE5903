package EE5903;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;

public class Tester{
	static Random randomGenerator = new Random();
	public static void main(String [] args) {
//		System.out.print(randomGenerator.nextGaussian());
		
		
//		Map<String, Person> people = new HashMap<String, Person>();
//		Person jim = new Person("Jim", 25);
//		Person scott = new Person("Scott", 28);
//		Person anna = new Person("Anna", 23);
//
//		people.put(jim.getName(), jim);
//		people.put(scott.getName(), scott);
//		people.put(anna.getName(), anna);
//
//		// not yet sorted
//		List<Person> peopleByAge = new ArrayList<>(people.values());
//
//		Collections.sort(peopleByAge, new Comparator<Person>() {
//			@Override
//			public int compare(Person c1, Person c2) {
//				if(people.get(c1.getName()).getAge()>people.get(c2.getName()).getAge()) {
//					return 1;
//				}
//				else {
//					return -1;
//				} 
////				return people.get(c1.getName()).getAge()-people.get(c2.getName()).getAge();
//			}
//		});
////		Collections.sort(peopleByAge, Comparator.comparing(Person::getAge));
//		for (Person p : peopleByAge) {
//		    System.out.println(p.getName() + "\t" + p.getAge());
//		}
		
//		ZipfGenerator zipfGenerator = new ZipfGenerator(10,1);
//		for(int i=0; i<100; i++) {
//			System.out.println(zipfGenerator.next());
//		}
		
		WeibullDistribution weibull= new WeibullDistribution(1,1);
		for(int i=0; i<100; i++) {
			System.out.println(i/10.0+" "+weibull.density(-i*0.1/10));
		}
	}
}