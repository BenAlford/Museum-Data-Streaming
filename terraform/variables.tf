variable "rds_instance_name" {
  description = "Value of the RDS instance's Name tag."
  type        = string
  default     = "c26-ben-lmnh-rds"
}

variable "rds_instance_type" {
  description = "The RDS instance's type."
  type        = string
  default     = "db.t3.micro"
}

variable "vpc_name" {
  description = "Value of the existing VPC's Name tag."
  type        = string
  default     = "your_vpc_here"
}

variable "public_subnet_1_name" {
  description = "Value of the first public subnet's Name tag."
  type        = string
  default     = "your_public_subnet_1_here"
}

variable "public_subnet_2_name" {
  description = "Value of the second public subnet's Name tag."
  type        = string
  default     = "your_public_subnet_2_here"
}

variable "db_username" {
  description = "The username for the RDS database."
  type        = string
}

variable "db_password" {
  description = "The password for the RDS database."
  type        = string
}

variable "ec2_instance_type" {
  description = "The bastion EC2 instance's type."
  type        = string
  default     = "t3.micro"
}

variable "key_name" {
  description = "Name of the existing EC2 key pair for the bastion."
  type        = string
  default     = "your_key_name_here"
}

variable "ssh_allowed_cidrs" {
  description = "CIDR blocks allowed to SSH into the bastion."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "rds_allowed_cidrs" {
  description = "CIDR blocks allowed to connect directly to the RDS instance."
  type        = list(string)
  default = [
    "145.224.193.0/24",
    "145.224.217.0/24",
    "141.163.208.0/23",
    "141.163.200.0/23",
    "141.163.214.0/24",
    "141.163.195.0/24",
    "141.163.204.0/23",
    "141.163.207.0/24",
    "141.163.216.0/24",
    "141.163.203.0/24",
    "141.163.196.0/23",
    "141.163.192.0/23",
    "141.163.199.0/24",
    "141.163.212.0/24",
    "155.226.152.0/23",
    "145.224.208.0/23",
    "145.224.211.0/24",
    "145.224.198.0/24",
    "145.224.215.0/24",
    "145.224.204.0/23",
    "145.224.199.0/24",
    "145.224.196.0/24",
    "145.224.202.0/23",
    "145.224.200.0/23",
    "145.224.219.0/24",
    "145.224.212.0/24",
    "145.224.206.0/23",
    "145.224.192.0/24",
    "145.224.194.0/24",
    "155.226.188.0/23",
    "155.226.144.0/22",
    "155.226.137.0/24",
    "155.226.156.0/23",
    "155.226.186.0/23",
    "155.226.136.0/24",
    "155.226.128.0/21",
    "155.226.151.0/24",
    "129.77.12.0/24",
    "129.77.13.0/24",
  ]
}
