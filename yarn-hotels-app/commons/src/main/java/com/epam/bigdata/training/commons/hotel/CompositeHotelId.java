package com.epam.bigdata.training.commons.hotel;

import java.util.Objects;

/**
 * Composite hotel id, consiting of country and market
 * and representing unique hotel.
 */
public class CompositeHotelId {

    private final String country;
    private final String market;

    public CompositeHotelId(String country, String market) {
        this.country = country;
        this.market = market;
    }

    public String getCountry() {
        return country;
    }

    public String getMarket() {
        return market;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeHotelId that = (CompositeHotelId) o;
        return Objects.equals(country, that.country) &&
                Objects.equals(market, that.market);
    }

    @Override
    public int hashCode() {

        return Objects.hash(country, market);
    }

    @Override
    public String toString() {
        return "HotelCompositeKey{" +
                "country='" + country + '\'' +
                ", market='" + market + '\'' +
                '}';
    }

}
