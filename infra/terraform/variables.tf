variable "security_group_id" {
    description = "Id for the security groups used for the glue connection"
    type = string
    default="sg-6dde6421"
}

variable "subnet_id" {
    description = "Id for the subnets used for the glue connection"
    type = string
    default="subnet-611d4a2b"
}

variable "availability_zone" {
    description = "Availability zone name for the subnet for the glue connection"
    type = string
    default = "us-east-1a"
}