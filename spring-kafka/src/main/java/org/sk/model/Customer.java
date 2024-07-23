package org.sk.model;


import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Customer {

    private int id;
    private String name;
    private String streetAddress;
    private String city;
    private String state;
    private String country;
    private String zipCode;

}
