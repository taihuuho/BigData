package mum.edu.bigdata;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Functions {
	public static Function<String, Customer> PARSE_CUSTOMER_LINE = new Function<String, Customer>() {
		private static final long serialVersionUID = 959487686373862849L;

		@Override
		public Customer call(String customerLine) throws Exception {
			return Customer.parseFromCustomerLine(customerLine);
		}
	};

	public static Function<Customer, Long> GET_CUSTOMER_ETIME = new Function<Customer, Long>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Long call(Customer customer) throws Exception {
			return new Long(customer.getTimeElapsed());
		}
	};

	public static PairFunction<Customer, String, Long> GET_CUSTOMER_AMOUNT = new PairFunction<Customer, String, Long>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Long> call(Customer customer) throws Exception {
			return new Tuple2<String, Long>(customer.getFirstName() + " "
					+ customer.getLastName(), customer.getAmount());
		}
	};

	public static Function2<Long, Long, Long> SUM_REDUCER = new Function2<Long, Long, Long>() {
		private static final long serialVersionUID = 399999920650666013L;

		@Override
		public Long call(Long a, Long b) throws Exception {
			return a + b;
		}
	};

	public static Function<Tuple2<String, Long>, Boolean> FILTER_AMOUNT_GREATER_10000 = new Function<Tuple2<String, Long>, Boolean>() {
		private static final long serialVersionUID = 3531695370880560298L;

		@Override
		public Boolean call(Tuple2<String, Long> tuple) throws Exception {
			return tuple._2() >= 10000;
		}
	};

	public static class LongComparator implements Comparator<Long>,
			Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3152398743080165674L;

		@Override
		public int compare(Long a, Long b) {
			if (a > b)
				return 1;
			if (a.equals(b))
				return 0;
			return -1;
		}
	}

	public static Comparator<Long> LONG_NATURAL_ORDER_COMPARATOR = new LongComparator();

	public static Function<String, Customer> PARSE_LOG_LINE = new Function<String, Customer>() {

		/**
					 * 
					 */
		private static final long serialVersionUID = 1L;

		@Override
		public Customer call(String customerLine) throws Exception {
			return Customer.parseFromCustomerLine(customerLine);
		}
	};

}
